const axios = require('axios');
const cheerio = require('cheerio');
const marked = require('marked');
const duckdb = require('duckdb-async');

async function fetchMd() {
  const response = await axios.get('https://raw.githubusercontent.com/leomaurodesenv/game-datasets/refs/heads/master/README.md');
  return response.data;
}

function convertMdToJson(md) {
  const tokens = marked.lexer(md);
  const json = {
    title: '',
    sections: []
  };

  let currentSection = null;

  for (const token of tokens) {
    if (token.type === 'heading' && token.depth === 1) {
      json.title = token.text;
    } else if (token.type === 'heading' && token.depth === 2) {
      currentSection = {
        name: token.text,
        items: []
      };
      json.sections.push(currentSection);
    } else if (token.type === 'list' && currentSection) {
      for (const item of token.items) {
        const linkToken = item.tokens.find(t => t.type === 'link');
        if (linkToken) {
          currentSection.items.push({
            name: linkToken.text,
            url: linkToken.href,
            description: item.text.replace(`[${linkToken.text}](${linkToken.href})`, '').trim()
          });
        }
      }
    }
  }

  return json;
}

async function getOgData(url) {
  try {
    const response = await axios.get(url);
    const $ = cheerio.load(response.data);
    
    const ogTitle = $('meta[property="og:title"]').attr('content');
    const ogDescription = $('meta[property="og:description"]').attr('content');
    const ogImage = $('meta[property="og:image"]').attr('content');

    return {
      ogTitle,
      ogDescription,
      ogImage
    };
  } catch (error) {
    console.error(`Error fetching OG data for ${url}: ${error.message}`);
    return {};
  }
}

async function updateJsonWithOgData(json) {
  for (const section of json.sections) {
    for (const item of section.items) {
      const ogData = await getOgData(item.url);
      Object.assign(item, ogData);
    }
  }
  return json;
}

async function ingestJsonToDuckDb(json) {
  const db = await duckdb.createDatabase(':memory:');
  const conn = await db.connect();

  await conn.query(`
    CREATE TABLE game_datasets (
      section VARCHAR,
      name VARCHAR,
      url VARCHAR,
      description VARCHAR,
      og_title VARCHAR,
      og_description VARCHAR,
      og_image VARCHAR
    )
  `);

  for (const section of json.sections) {
    for (const item of section.items) {
      await conn.query(`
        INSERT INTO game_datasets (section, name, url, description, og_title, og_description, og_image)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `, [section.name, item.name, item.url, item.description, item.ogTitle, item.ogDescription, item.ogImage]);
    }
  }

  const result = await conn.query('SELECT * FROM game_datasets');
  console.log('Data ingested into DuckDB:', result);

  await conn.close();
  await db.close();
}

async function main() {
  const md = await fetchMd();
  let json = convertMdToJson(md);
  json = await updateJsonWithOgData(json);
  // await ingestJsonToDuckDb(json);

  // Save the processed data
  const fs = require('fs');
  fs.writeFileSync('processed-data.json', JSON.stringify(json, null, 2));
}

main().catch(console.error);
