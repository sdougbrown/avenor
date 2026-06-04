const fs = require('node:fs');
const path = require('node:path');

const pkg = JSON.parse(fs.readFileSync('package.json', 'utf8'));

const siblings = {};
for (const dir of fs.readdirSync('..')) {
  const p = path.join('..', dir, 'package.json');
  if (!fs.existsSync(p)) continue;
  const siblingPkg = JSON.parse(fs.readFileSync(p, 'utf8'));
  siblings[siblingPkg.name] = siblingPkg;
}

const depFields = ['dependencies', 'devDependencies', 'peerDependencies', 'optionalDependencies'];

for (const field of depFields) {
  const deps = pkg[field];
  if (!deps) continue;

  for (const [name, spec] of Object.entries(deps)) {
    if (!spec.startsWith('workspace:')) continue;

    const sibling = siblings[name];
    if (sibling) {
      deps[name] = '^' + sibling.version;
    } else {
      process.stderr.write(`Error: workspace dependency "${name}" not found in sibling packages\n`);
      process.exit(1);
    }
  }
}

fs.writeFileSync('package.json', JSON.stringify(pkg, null, 2) + '\n');
