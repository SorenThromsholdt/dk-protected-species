const fs = require('fs');
const path = require('path');

const ALLOWED_PROTECTION = ["fredet", "bilag i", "bilag ii", "bilag iv", "bilag v", "fugle bilag i", "bilag 5"];
const SPECIES_LIST = JSON.parse(fs.readFileSync('bilag_iv.json', 'utf8')).filter(s => {
    if (!s.protection) return false;
    return s.protection.some(p => 
        ALLOWED_PROTECTION.some(allowed => p.toLowerCase().includes(allowed))
    );
});

const DATA_DIR = 'data';
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

// GBIF maximum limit for occurrence search is 300
const FETCH_LIMIT = 300; 
const CONCURRENT_YEARS = 5; // Fetch 5 years at once for a species

async function throttledFetch(url) {
    for (let i = 0; i < 10; i++) {
        try {
            const res = await fetch(url, { headers: { 'User-Agent': 'Danish-Species-Map/2.0-Aggressive' } });
            if (res.status === 429) { 
                const wait = 5000 * (i + 1);
                await new Promise(r => setTimeout(r, wait)); 
                continue; 
            }
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            return await res.json();
        } catch (err) {
            await new Promise(r => setTimeout(r, 2000));
        }
    }
}

async function fetchYearRange(taxonKey, species, year) {
    let offset = 0;
    let results = [];
    let hasMore = true;

    while (hasMore) {
        const params = new URLSearchParams({
            taxonKey: taxonKey,
            country: 'DK',
            hasCoordinate: 'true',
            occurrenceStatus: 'PRESENT',
            limit: FETCH_LIMIT,
            offset: offset,
            year: year
        });

        const data = await throttledFetch(`https://api.gbif.org/v1/occurrence/search?${params.toString()}`);
        if (!data || !data.results || data.results.length === 0) break;

        data.results.forEach(r => {
            if (!r.decimalLatitude || !r.decimalLongitude) return;
            const key = `${Math.floor(r.decimalLatitude)}_${Math.floor(r.decimalLongitude)}`;
            
            const record = {
                lat: Math.round(r.decimalLatitude * 10000) / 10000,
                lng: Math.round(r.decimalLongitude * 10000) / 10000,
                species: species.scientific_name,
                commonName: species.danish_name,
                date: r.eventDate ? r.eventDate.split('T')[0] : null,
                protection: species.protection,
                id: r.key
            };

            if (!results[key]) results[key] = [];
            results[key].push(record);
        });

        offset += FETCH_LIMIT;
        hasMore = !data.endOfRecords && offset < 100000;
    }
    return results;
}

async function fetchSpecies(species) {
    const match = await throttledFetch(`https://api.gbif.org/v1/species/match?name=${encodeURIComponent(species.scientific_name)}&country=DK`);
    const taxonKey = match ? match.usageKey : null;
    if (!taxonKey) return;

    console.log(`\nAggressiv hentning: ${species.danish_name}...`);
    
    const currentYear = new Date().getFullYear();
    const years = [];
    for (let y = 1900; y <= currentYear; y++) years.push(y);
    years.push("*,1899"); // Everything before 1900

    let speciesTotal = 0;

    // Process years in chunks
    for (let i = 0; i < years.length; i += CONCURRENT_YEARS) {
        const chunk = years.slice(i, i + CONCURRENT_YEARS);
        const chunkResults = await Promise.all(chunk.map(y => fetchYearRange(taxonKey, species, y)));
        
        const byTile = {};
        chunkResults.forEach(yearResults => {
            for (const [tileKey, items] of Object.entries(yearResults)) {
                if (!byTile[tileKey]) byTile[tileKey] = [];
                byTile[tileKey].push(...items);
                speciesTotal += items.length;
            }
        });

        // Save tile data immediately
        for (const [key, items] of Object.entries(byTile)) {
            const file = path.join(DATA_DIR, `${key}.json`);
            let existing = [];
            if (fs.existsSync(file)) {
                try { existing = JSON.parse(fs.readFileSync(file, 'utf8')); } catch(e) {}
            }
            const combined = [...items, ...existing];
            const unique = [];
            const seen = new Set();
            for (const x of combined) { if (!seen.has(x.id)) { seen.add(x.id); unique.push(x); } }
            fs.writeFileSync(file, JSON.stringify(unique));
        }
        process.stdout.write(`\r  -> Status: ${speciesTotal} fund hentet...`);
    }
    console.log(`\r  -> Færdig med ${species.danish_name}: ${speciesTotal} fund i alt.`);
}

async function main() {
    console.log("STARTER TOTAL HENTNING (INGEN GRÆNSER)...");
    
    for (let i = 0; i < SPECIES_LIST.length; i++) {
        await fetchSpecies(SPECIES_LIST[i]);
        console.log(`Samlet fremdrift: ${i + 1}/${SPECIES_LIST.length} arter.`);
    }
    console.log("\n*** ALLE DATA ER HENTET UDEN BEGRÆNSNINGER! ***");
}

main().catch(console.error);
