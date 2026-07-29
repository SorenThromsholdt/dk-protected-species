const test = require('node:test');
const assert = require('node:assert/strict');
const { shouldSkipSpecies } = require('../fetch_gbif.js');

test('does not skip species that were previously marked complete unless explicitly requested', () => {
  assert.equal(
    shouldSkipSpecies('Acrocephalus paludicola', { 'Acrocephalus paludicola': 'COMPLETE' }),
    false
  );

  assert.equal(
    shouldSkipSpecies('Acrocephalus paludicola', { 'Acrocephalus paludicola': 'COMPLETE' }, { skipCompleted: true }),
    true
  );
});
