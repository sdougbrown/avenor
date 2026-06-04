import { describe, it, expect, afterEach } from "bun:test";
import { mkdtempSync, mkdirSync, readFileSync, existsSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { $ } from "bun";

let tempDir;

afterEach(() => {
  if (tempDir && existsSync(tempDir)) rmSync(tempDir, { recursive: true });
});

function setupWorkspace(depVersion = "0.3.0") {
  tempDir = mkdtempSync(join(tmpdir(), "avenor-test-"));
  const pkgsDir = join(tempDir, "packages");
  mkdirSync(pkgsDir, { recursive: true });
  // core package (the workspace dependency)
  const coreDir = join(pkgsDir, "core");
  mkdirSync(coreDir, { recursive: true });
  const corePkg = { name: "@dougbots/avenor-core", version: depVersion };
  writeFileSync(join(coreDir, "package.json"), JSON.stringify(corePkg, null, 2) + "\n");
  return tempDir;
}

function createPackageDir(tempDir, name) {
  const dir = join(tempDir, "packages", name);
  mkdirSync(dir, { recursive: true });
  return dir;
}

function writePackage(dir, pkg) {
  writeFileSync(join(dir, "package.json"), JSON.stringify(pkg, null, 2) + "\n");
}

describe("resolve-workspace-deps", () => {
  it("resolves workspace:* to ^version", async () => {
    const root = setupWorkspace("0.3.0");
    const pkgDir = createPackageDir(root, "mcp");
    writePackage(pkgDir, {
      name: "test-pkg",
      version: "1.0.0",
      dependencies: {
        "@dougbots/avenor-core": "workspace:*",
        "zod": "^4.4.3",
      },
    });

    const result = await $`bun ${join(import.meta.dirname, "resolve-workspace-deps.js")}`.cwd(pkgDir).quiet();
    expect(result.exitCode).toBe(0);

    const pkg = JSON.parse(readFileSync(join(pkgDir, "package.json"), "utf8"));
    expect(pkg.dependencies["@dougbots/avenor-core"]).toBe("^0.3.0");
    expect(pkg.dependencies.zod).toBe("^4.4.3");
  });

  it("leaves non-workspace deps untouched", async () => {
    const root = setupWorkspace();
    const pkgDir = createPackageDir(root, "opencode");
    writePackage(pkgDir, {
      name: "test-pkg",
      version: "1.0.0",
      dependencies: {
        "@dougbots/avenor-core": "workspace:*",
        "zod": "^4.4.3",
        "some-other-dep": "~5.0.0",
      },
    });

    const result = await $`bun ${join(import.meta.dirname, "resolve-workspace-deps.js")}`.cwd(pkgDir).quiet();
    expect(result.exitCode).toBe(0);

    const pkg = JSON.parse(readFileSync(join(pkgDir, "package.json"), "utf8"));
    expect(pkg.dependencies.zod).toBe("^4.4.3");
    expect(pkg.dependencies["some-other-dep"]).toBe("~5.0.0");
  });

  it("handles no workspace deps gracefully (no-op)", async () => {
    const root = setupWorkspace();
    const pkgDir = createPackageDir(root, "mcp");
    writePackage(pkgDir, {
      name: "test-pkg",
      version: "1.0.0",
      dependencies: { zod: "^4.4.3" },
    });

    const pkgBefore = JSON.stringify(JSON.parse(readFileSync(join(pkgDir, "package.json"), "utf8")));

    const result = await $`bun ${join(import.meta.dirname, "resolve-workspace-deps.js")}`.cwd(pkgDir).quiet();
    expect(result.exitCode).toBe(0);

    const pkgAfter = JSON.stringify(JSON.parse(readFileSync(join(pkgDir, "package.json"), "utf8")));
    expect(pkgAfter).toBe(pkgBefore);
  });

  it("handles missing dependencies field", async () => {
    const root = setupWorkspace();
    const pkgDir = createPackageDir(root, "mcp");
    const pkg = { name: "test-pkg", version: "1.0.0" };
    writeFileSync(join(pkgDir, "package.json"), JSON.stringify(pkg, null, 2) + "\n");

    const result = await $`bun ${join(import.meta.dirname, "resolve-workspace-deps.js")}`.cwd(pkgDir).quiet();
    expect(result.exitCode).toBe(0);

    const pkgAfter = JSON.parse(readFileSync(join(pkgDir, "package.json"), "utf8"));
    expect(pkgAfter).toEqual(pkg);
  });

  it("resolves from package name even when dir name differs", async () => {
    const root = setupWorkspace("2.1.0");
    const pkgDir = createPackageDir(root, "opencode");
    // dir name is 'opencode' but the only sibling package is in 'core' — script matches by package.json name
    writePackage(pkgDir, {
      name: "test-pkg",
      version: "1.0.0",
      dependencies: {
        "@dougbots/avenor-core": "workspace:*",
      },
    });

    const result = await $`bun ${join(import.meta.dirname, "resolve-workspace-deps.js")}`.cwd(pkgDir).quiet();
    expect(result.exitCode).toBe(0);

    const pkg = JSON.parse(readFileSync(join(pkgDir, "package.json"), "utf8"));
    expect(pkg.dependencies["@dougbots/avenor-core"]).toBe("^2.1.0");
  });
});
