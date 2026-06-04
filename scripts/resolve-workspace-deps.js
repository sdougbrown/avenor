const fs = require('node:fs');
const path = require('node:path');

const pkg = JSON.parse(fs.readFileSync('package.json', 'utf8'));

const depFields = ['dependencies', 'devDependencies', 'peerDependencies', 'optionalDependencies'];

for (const field of depFields) {
  const deps = pkg[field];
  if (!deps) continue;

  for (const [name, spec] of Object.entries(deps)) {
    if (!spec.startsWith('workspace:')) continue;

    let resolved = false;
    for (const dir of fs.readdirSync('..')) {
      const p = path.join('..', dir, 'package.json');
      if (fs.existsSync(p) && JSON.parse(fs.readFileSync(p, 'utf8')).name === name) {
        deps[name] = '^' + JSON.parse(fs.readFileSync(p, 'utf8')).version;
        resolved = true;
        break;
      }
    }

    if (!resolved) {
      process.stderr.write(`Error: workspace dependency "${name}" not found in sibling packages\n`);
      process.exit(1);
    }
  }
}

fs.writeFileSync('package.json', JSON.stringify(pkg, null, 2) + '\n');
