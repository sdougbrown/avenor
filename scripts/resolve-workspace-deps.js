const fs = require('node:fs');
const path = require('node:path');

const pkg = JSON.parse(fs.readFileSync('package.json', 'utf8'));

for (const [name, spec] of Object.entries(pkg.dependencies || {})) {
  if (spec.startsWith('workspace:')) {
    for (const dir of fs.readdirSync('..')) {
      const p = path.join('..', dir, 'package.json');
      if (fs.existsSync(p) && JSON.parse(fs.readFileSync(p, 'utf8')).name === name) {
        pkg.dependencies[name] = '^' + JSON.parse(fs.readFileSync(p, 'utf8')).version;
        break;
      }
    }
  }
}

fs.writeFileSync('package.json', JSON.stringify(pkg, null, 2) + '\n');
