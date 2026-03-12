const fs = require('fs');
const path = require('path');

// Configurations
const DATA_DIR = 'data';
const STATE_FILE = 'fetch_state.json';
const ALLOWED_PROTECTION = ["fredet", "bilag i", "bilag ii", "bilag iv", "bilag v", "fugle bilag i", "bilag 5"];
const FETCH_LIMIT = 1000; // Optimized GBIF search limit
const MAX_SPECIES_CONCURRENCY = 10; // More aggressive concurrency

// Initialize
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

const SPECIES_LIST = JSON.parse(fs.readFileSync('bilag_iv.json', 'utf8')).filter(s => {
    if (!s.protection) return false;
    return s.protection.some(p => 
        ALLOWED_PROTECTION.some(allowed => p.toLowerCase().includes(allowed))
    );
});

let fetchState = {};
if (fs.existsSync(STATE_FILE)) {
    try {
        fetchState = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    } catch (e) {
        console.error("Could not read state file, starting fresh.");
    }
}

function saveState() {
    fs.writeFileSync(STATE_FILE, JSON.stringify(fetchState, null, 2));
}

async function throttledFetch(url) {
    for (let i = 0; i < 15; i++) {
        try {
            const res = await fetch(url, { 
                headers: { 'User-Agent': 'Danish-Species-Map/4.0-Fast' },
                signal: AbortSignal.timeout(15000) 
            });
            if (res.status === 429) { 
                const wait = 5000 * (i + 1);
                await new Promise(r => setTimeout(r, wait)); 
                continue; 
            }
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            return await res.json();
        } catch (err) {
            await new Promise(r => setTimeout(r, 3000));
        }
    }
}

async function fetchAllForSpecies(taxonKey, species) {
    let offset = 0;
    let totalAdded = 0;
    let hasMore = true;
    const speciesResultsByTile = {};

    while (hasMore) {
        const params = new URLSearchParams({
            taxonKey: taxonKey,
            country: 'DK',
            hasCoordinate: 'true',
            occurrenceStatus: 'PRESENT',
            limit: FETCH_LIMIT,
            offset: offset
        });

        const data = await throttledFetch(`https://api.gbif.org/v1/occurrence/search?${params.toString()}`);
        if (!data || !data.results || data.results.length === 0) break;

        data.results.forEach(r => {
            if (!r.decimalLatitude || !r.decimalLongitude) return;
            const tileKey = `${Math.floor(r.decimalLatitude)}_${Math.floor(r.decimalLongitude)}`;
            
            const record = {
                lat: Math.round(r.decimalLatitude * 10000) / 10000,
                lng: Math.round(r.decimalLongitude * 10000) / 10000,
                species: species.scientific_name,
                commonName: species.danish_name,
                date: r.eventDate ? r.eventDate.split('T')[0] : null,
                protection: species.protection,
                id: r.key
            };

            if (!speciesResultsByTile[tileKey]) speciesResultsByTile[tileKey] = [];
            speciesResultsByTile[tileKey].push(record);
            totalAdded++;
        });

        offset += FETCH_LIMIT;
        // GBIF max offset is 100,000 for search
        hasMore = !data.endOfRecords && offset < 100000;
    }

    // Write all tiles for this species once
    for (const [tileKey, items] of Object.entries(speciesResultsByTile)) {
        const file = path.join(DATA_DIR, `${tileKey}.json`);
        let existing = [];
        if (fs.existsSync(file)) {
            try { existing = JSON.parse(fs.readFileSync(file, 'utf8')); } catch(e) {}
        }
        
        // Merge & Deduplicate
        const combined = [...items, ...existing];
        const unique = [];
        const seen = new Set();
        for (const x of combined) {
            if (!seen.has(x.id)) {
                seen.add(x.id);
                unique.push(x);
            }
        }
        fs.writeFileSync(file, JSON.stringify(unique));
    }

    return totalAdded;
}

async function processSpecies(species) {
    // Check if already fully fetched (state tracking)
    if (fetchState[species.scientific_name] === "COMPLETE") {
        return 0;
    }

    const match = await throttledFetch(`https://api.gbif.org/v1/species/match?name=${encodeURIComponent(species.scientific_name)}&country=DK`);
    const taxonKey = match ? match.usageKey : null;
    if (!taxonKey) {
        console.warn(`\n[WARN] No GBIF match for: ${species.scientific_name}`);
        return 0;
    }

    const total = await fetchAllForSpecies(taxonKey, species);
    
    // Mark as complete
    fetchState[species.scientific_name] = "COMPLETE";
    saveState();
    
    return total;
}

function printProgressBar(current, total, lastSpecies, count, barSize = 40) {
    const percentage = (current / total) * 100;
    const completedSize = Math.floor((current / total) * barSize);
    const remainingSize = barSize - completedSize;
    const bar = "█".repeat(completedSize) + "░".repeat(remainingSize);
    process.stdout.write(`\r[${bar}] ${percentage.toFixed(1)}% | ${current}/${total} | Last: ${lastSpecies.substring(0, 20)} (+${count})`);
}

async function main() {
    console.log(`Starting Optimized Data Fetch (All-at-once pagination)...`);
    console.log(`Target: ${SPECIES_LIST.length} species`);
    
    let completedCount = 0;
    const totalCount = SPECIES_LIST.length;

    const queue = [...SPECIES_LIST];
    const workers = [];

    async function worker() {
        while (queue.length > 0) {
            const species = queue.shift();
            try {
                const count = await processSpecies(species);
                completedCount++;
                printProgressBar(completedCount, totalCount, species.danish_name, count);
            } catch (err) {
                console.error(`\nError fetching ${species.scientific_name}:`, err.message);
                completedCount++; // Still move forward
            }
        }
    }

    // Initial progress bar
    printProgressBar(0, totalCount, "Starting...", 0);

    // Start concurrent workers
    for (let i = 0; i < MAX_SPECIES_CONCURRENCY; i++) {
        workers.push(worker());
    }

    await Promise.all(workers);
    console.log("\n\n*** DATA UPDATE COMPLETE! (Optimized Method) ***");
}

main().catch(console.error);
