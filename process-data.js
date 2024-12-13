const axios = require('axios');
const cheerio = require('cheerio');
const duckdb = require('duckdb-async');
const fs = require('fs').promises;

async function fetchMd() {
  const response = await axios.get('https://raw.githubusercontent.com/leomaurodesenv/game-datasets/refs/heads/master/README.md');
  return response.data;
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
    if (section.items) {
      for (const item of section.items) {
        if (item.link) {
          const ogData = await getOgData(item.link);
          Object.assign(item, ogData);
        }
      }
    }
    if (section.subsections) {
      for (const subsection of section.subsections) {
        for (const item of subsection.items) {
          if (item.link) {
            const ogData = await getOgData(item.link);
            Object.assign(item, ogData);
          }
        }
      }
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
      subsection VARCHAR,
      name VARCHAR,
      description VARCHAR,
      link VARCHAR,
      og_title VARCHAR,
      og_description VARCHAR,
      og_image VARCHAR
    )
  `);

  for (const section of json.sections) {
    if (section.items) {
      for (const item of section.items) {
        await conn.query(`
          INSERT INTO game_datasets (section, subsection, name, description, link, og_title, og_description, og_image)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `, [section.name, null, item.name, item.description, item.link, item.ogTitle, item.ogDescription, item.ogImage]);
      }
    }
    if (section.subsections) {
      for (const subsection of section.subsections) {
        for (const item of subsection.items) {
          await conn.query(`
            INSERT INTO game_datasets (section, subsection, name, description, link, og_title, og_description, og_image)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          `, [section.name, subsection.name, item.name, item.description, item.link, item.ogTitle, item.ogDescription, item.ogImage]);
        }
      }
    }
  }

  const result = await conn.query('SELECT * FROM game_datasets');
  console.log('Data ingested into DuckDB:', result);

  await conn.close();
  await db.close();
}

async function main() {
  // Load the existing JSON structure
  const jsonContent = await fs.readFile('game-datasets.json', 'utf8');
  let json = JSON.parse(jsonContent);

  // Update JSON with OG data
  json = await updateJsonWithOgData(json);

  // Ingest data into DuckDB
  // await ingestJsonToDuckDb(json);

  // Save the updated JSON
  await fs.writeFile('processed-game-datasets.json', JSON.stringify(json, null, 2));
}

main().catch(console.error);
