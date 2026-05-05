const fs = require('fs');

let file = fs.readFileSync('web/features/journeys/01_supplier_prospecting.feature', 'utf-8');

file = file.replace(
  `  @planned @market
  Scenario: Search for an object opens an aggregated market page`,
  `  @green @market
  Scenario: Search for an object opens an aggregated market page`
);

fs.writeFileSync('web/features/journeys/01_supplier_prospecting.feature', file);
