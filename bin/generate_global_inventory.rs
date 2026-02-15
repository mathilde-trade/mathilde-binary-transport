use std::collections::{HashMap, HashSet};
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct ModuleArtifacts {
    inventory_md: String,
    scope_md: Option<String>,
    spec_mds: Vec<String>,
    evidence_logs: Vec<String>,
    bench_logs: Vec<String>,
    math_reviews: Vec<String>,
    tests_dir: Option<String>,
    benches: Vec<String>,
}

fn parse_flag_value(args: &[String], flag: &str) -> Option<String> {
    // supports:
    // - --flag=value
    // - --flag value
    for (i, a) in args.iter().enumerate() {
        if let Some(rest) = a.strip_prefix(&(flag.to_string() + "=")) {
            return Some(rest.to_string());
        }
        if a == flag {
            return args.get(i + 1).cloned();
        }
    }
    None
}

fn repo_root_from_args_or_cwd(args: &[String]) -> Result<PathBuf, String> {
    // This tool is designed to run as a standalone `rustc`-compiled binary, so we do not
    // rely on `env!("CARGO_MANIFEST_DIR")`.
    if let Some(v) = parse_flag_value(args, "--repo-root") {
        let p = PathBuf::from(v);
        return Ok(p);
    }
    env::current_dir().map_err(|e| format!("current_dir failed: {e}"))
}

fn rel_path(repo_root: &Path, path: &Path) -> Result<String, String> {
    let rel = path
        .canonicalize()
        .map_err(|e| format!("canonicalize failed for {path:?}: {e}"))?
        .strip_prefix(
            repo_root
                .canonicalize()
                .map_err(|e| format!("canonicalize failed for repo_root {repo_root:?}: {e}"))?,
        )
        .map_err(|e| format!("strip_prefix failed for {path:?}: {e}"))?
        .to_string_lossy()
        .to_string();
    Ok(rel.replace('\\', "/"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ComponentKind {
    Crate,
    Service,
}

#[derive(Debug, Clone)]
struct ComponentInventory {
    kind: ComponentKind,
    name: String,
    inventory_path: PathBuf,
    source_root: PathBuf,
}

fn discover_component_inventories(repo_root: &Path) -> Result<Vec<ComponentInventory>, String> {
    let mut out: Vec<ComponentInventory> = Vec::new();

    // Root crate discovery: a single-crate repo may keep `Cargo.toml` at repo root.
    // If an inventory exists at the root, treat it as a crate component.
    {
        let manifest = repo_root.join("Cargo.toml");
        let inv = repo_root.join("src").join("docs").join("inventory.md");
        let inv_legacy = repo_root.join("docs").join("inventory.md");
        let inv = if inv.is_file() { inv } else { inv_legacy };
        if manifest.is_file() && inv.is_file() {
            let name = repo_root
                .file_name()
                .unwrap_or_else(|| OsStr::new(""))
                .to_string_lossy()
                .to_string();
            out.push(ComponentInventory {
                kind: ComponentKind::Crate,
                name: if name.is_empty() { "root".to_string() } else { name },
                inventory_path: inv,
                source_root: repo_root.to_path_buf(),
            });
        }
    }

    // Primary discovery: workspace-style crates at repo root, e.g. `crates/*` or `some-lib/`.
    for entry in fs::read_dir(repo_root).map_err(|e| format!("read_dir failed: {e}"))? {
        let entry = entry.map_err(|e| format!("read_dir entry failed: {e}"))?;
        let p = entry.path();
        if !p.is_dir() {
            continue;
        }
        let name = p
            .file_name()
            .unwrap_or_else(|| OsStr::new(""))
            .to_string_lossy()
            .to_string();
        if name.is_empty() {
            continue;
        }
        // Skip known non-crate roots.
        if matches!(
            name.as_str(),
            ".git"
                | ".cargo"
                | ".serena"
                | ".husky"
                | "target"
                | "node_modules"
                | "protocols"
                | "bin"
                | "dev"
                | "deprecated"
        ) {
            continue;
        }
        let manifest = p.join("Cargo.toml");
        if !manifest.is_file() {
            continue;
        }
        let inv = p.join("src").join("docs").join("inventory.md");
        let inv_legacy = p.join("docs").join("inventory.md");
        let inv = if inv.is_file() { inv } else { inv_legacy };
        if inv.is_file() {
            out.push(ComponentInventory {
                kind: ComponentKind::Crate,
                name,
                inventory_path: inv,
                source_root: p,
            });
        }
    }

    // Compatibility discovery: mono-repos that keep components under `crates/*` and `services/*`.
    let pairs = [
        (ComponentKind::Crate, repo_root.join("crates")),
        (ComponentKind::Service, repo_root.join("services")),
    ];
    for (kind, base) in pairs {
        if !base.is_dir() {
            continue;
        }
        for entry in fs::read_dir(&base).map_err(|e| format!("read_dir failed: {base:?}: {e}"))? {
            let entry = entry.map_err(|e| format!("read_dir entry failed: {e}"))?;
            let p = entry.path();
            if !p.is_dir() {
                continue;
            }
            let name = p
                .file_name()
                .unwrap_or_else(|| OsStr::new(""))
                .to_string_lossy()
                .to_string();
            if name.is_empty() {
                continue;
            }
            let inv = p.join("src").join("docs").join("inventory.md");
            let inv_legacy = p.join("docs").join("inventory.md");
            let inv = if inv.is_file() { inv } else { inv_legacy };
            if inv.is_file() {
                out.push(ComponentInventory {
                    kind,
                    name,
                    inventory_path: inv,
                    source_root: p,
                });
                continue;
            }
        }
    }

    out.sort_by(|a, b| (a.kind as u8, &a.name).cmp(&(b.kind as u8, &b.name)));
    Ok(out)
}

fn collect_artifacts(
    repo_root: &Path,
    component_root: &Path,
    component_inventory: &Path,
) -> Result<ModuleArtifacts, String> {
    let docs_dir = component_root.join("docs");
    let inventory_md = rel_path(repo_root, component_inventory)?;
    let scope_path = docs_dir.join("scope.md");
    let scope_md = if scope_path.is_file() {
        Some(rel_path(repo_root, &scope_path)?)
    } else {
        None
    };

    let mut spec_mds: Vec<String> = Vec::new();

    // TA layout: specs are commonly in `docs/*_SPEC.md`.
    if docs_dir.is_dir() {
        for entry in fs::read_dir(&docs_dir).map_err(|e| format!("read_dir docs failed: {e}"))? {
            let entry = entry.map_err(|e| format!("read_dir docs entry failed: {e}"))?;
            let p = entry.path();
            if !p.is_file() || p.extension() != Some(OsStr::new("md")) {
                continue;
            }
            let name = p
                .file_name()
                .unwrap_or_else(|| OsStr::new(""))
                .to_string_lossy()
                .to_string();
            if name.ends_with("_SPEC.md") {
                spec_mds.push(rel_path(repo_root, &p)?);
            }
        }
    }

    let specs_dir = docs_dir.join("specs");
    if specs_dir.is_dir() {
        for entry in fs::read_dir(&specs_dir).map_err(|e| format!("read_dir specs failed: {e}"))? {
            let entry = entry.map_err(|e| format!("read_dir specs entry failed: {e}"))?;
            let p = entry.path();
            if p.is_file() && p.extension() == Some(OsStr::new("md")) {
                spec_mds.push(rel_path(repo_root, &p)?);
            }
        }
    }
    spec_mds.sort();

    let mut evidence_logs: Vec<String> = Vec::new();
    let evidence_dir = docs_dir.join("evidence");
    if evidence_dir.is_dir() {
        for entry in
            fs::read_dir(&evidence_dir).map_err(|e| format!("read_dir evidence failed: {e}"))?
        {
            let entry = entry.map_err(|e| format!("read_dir evidence entry failed: {e}"))?;
            let p = entry.path();
            if p.is_file() && p.extension() == Some(OsStr::new("md")) {
                evidence_logs.push(rel_path(repo_root, &p)?);
            }
        }
    }
    evidence_logs.sort();

    let mut bench_logs: Vec<String> = Vec::new();
    if docs_dir.is_dir() {
        for entry in fs::read_dir(&docs_dir).map_err(|e| format!("read_dir docs failed: {e}"))? {
            let entry = entry.map_err(|e| format!("read_dir docs entry failed: {e}"))?;
            let p = entry.path();
            if !p.is_file() || p.extension() != Some(OsStr::new("md")) {
                continue;
            }
            let name = p
                .file_name()
                .unwrap_or_else(|| OsStr::new(""))
                .to_string_lossy()
                .to_lowercase();
            if name == "inventory.md" || name == "scope.md" {
                continue;
            }
            // Convention: operator-generated benchmark/perf logs are named with "bench" or "perf".
            if name.contains("bench") || name.contains("perf") {
                bench_logs.push(rel_path(repo_root, &p)?);
            }
        }
    }
    bench_logs.sort();

    let mut math_reviews: Vec<String> = Vec::new();
    let reviews_dir = docs_dir.join("reviews");
    if reviews_dir.is_dir() {
        for entry in
            fs::read_dir(&reviews_dir).map_err(|e| format!("read_dir reviews failed: {e}"))?
        {
            let entry = entry.map_err(|e| format!("read_dir reviews entry failed: {e}"))?;
            let p = entry.path();
            if p.is_file() && p.extension() == Some(OsStr::new("md")) {
                math_reviews.push(rel_path(repo_root, &p)?);
            }
        }
    }
    math_reviews.sort();

    let tests_dir_path = component_root.join("tests");
    let tests_dir = if tests_dir_path.is_dir() {
        Some(rel_path(repo_root, &tests_dir_path)?)
    } else {
        None
    };

    let benches_dir = component_root.join("benches");
    let mut benches: Vec<String> = Vec::new();
    if benches_dir.is_dir() {
        for entry in
            fs::read_dir(&benches_dir).map_err(|e| format!("read_dir benches failed: {e}"))?
        {
            let entry = entry.map_err(|e| format!("read_dir benches entry failed: {e}"))?;
            let p = entry.path();
            if p.is_file() && p.extension() == Some(OsStr::new("rs")) {
                benches.push(rel_path(repo_root, &p)?);
            }
        }
    }
    benches.sort();

    Ok(ModuleArtifacts {
        inventory_md,
        scope_md,
        spec_mds,
        evidence_logs,
        bench_logs,
        math_reviews,
        tests_dir,
        benches,
    })
}

fn parse_inventory_source_file_purposes(inventory_text: &str) -> HashMap<String, String> {
    // Accept only explicit file-purpose lines:
    // - `src/.../*.rs`: purpose... (crate-local; rebased by caller)
    // - `tests/.../*.rs`: purpose... (crate-local; rebased by caller)
    // - `benches/.../*.rs`: purpose... (crate-local; rebased by caller)
    // - `examples/.../*.rs`: purpose... (crate-local; rebased by caller)
    // - `bin/.../*.rs`: purpose... (crate-local; rebased by caller)
    // - `crates/<name>/.../*.rs`: purpose... (repo-relative; kept as-is)
    // - `services/<name>/.../*.rs`: purpose... (repo-relative; kept as-is)
    let mut map = HashMap::new();
    for raw_line in inventory_text.lines() {
        let line = raw_line.trim_start();
        if !line.starts_with("- `") {
            continue;
        }
        let rest = &line[3..];
        let Some(end_tick) = rest.find('`') else {
            continue;
        };
        let path = rest[..end_tick].trim();
        if !path.ends_with(".rs") {
            continue;
        }
        if path.starts_with('/')
            || path.starts_with("../")
            || path.contains("/../")
            || path.contains('\\')
        {
            continue;
        }
        if !(path.starts_with("crates/")
            || path.starts_with("services/")
            || path.starts_with("src/")
            || path.starts_with("tests/")
            || path.starts_with("benches/")
            || path.starts_with("examples/")
            || path.starts_with("bin/"))
        {
            continue;
        }
        let after = rest[end_tick + 1..].trim_start();
        if !after.starts_with(':') {
            continue;
        }
        let purpose = after[1..].trim();
        if purpose.is_empty() {
            continue;
        }
        map.insert(path.to_string(), purpose.to_string());
    }
    map
}

fn component_source_files(repo_root: &Path, component_root: &Path) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = vec![];
    let mut stack: Vec<PathBuf> = vec![component_root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).map_err(|e| format!("read_dir failed for {dir:?}: {e}"))? {
            let entry = entry.map_err(|e| format!("read_dir entry failed: {e}"))?;
            let p = entry.path();
            if p.is_dir() {
                let name = p.file_name().unwrap_or_else(|| OsStr::new(""));
                // Never scan docs/ (artifacts, not source).
                if name == OsStr::new("docs") {
                    continue;
                }
                // Never scan build artifacts/vendor dirs.
                if name == OsStr::new("target") || name == OsStr::new("node_modules") {
                    continue;
                }
                stack.push(p);
                continue;
            }
            if p.is_file() && p.extension() == Some(OsStr::new("rs")) {
                out.push(rel_path(repo_root, &p)?);
            }
        }
    }
    out.sort();
    Ok(out)
}

fn validate_exists(repo_root: &Path, rel: &str) -> Result<(), String> {
    let p = repo_root.join(rel);
    if !p.exists() {
        return Err(format!("referenced path does not exist: {rel}"));
    }
    Ok(())
}

fn find_crate_root(start: &Path) -> Result<PathBuf, String> {
    // Find the nearest ancestor containing `Cargo.toml`.
    let mut cur = start
        .canonicalize()
        .map_err(|e| format!("canonicalize failed for {start:?}: {e}"))?;
    loop {
        let candidate = cur.join("Cargo.toml");
        if candidate.is_file() {
            return Ok(cur);
        }
        if !cur.pop() {
            break;
        }
    }
    Err(format!(
        "could not locate crate root (missing Cargo.toml) from start={start:?}"
    ))
}

fn now_utc_iso_z() -> String {
    // Avoid extra deps; rely on RFC3339-ish output via chrono? not allowed.
    // We keep a stable placeholder with date only from system time.
    let now = std::time::SystemTime::now();
    let dt: dt::DateTime = now.into();
    dt.to_string()
}

mod dt {
    use std::time::{SystemTime, UNIX_EPOCH};

    pub struct DateTime {
        y: i32,
        m: u32,
        d: u32,
        hh: u32,
        mm: u32,
        ss: u32,
    }

    impl From<SystemTime> for DateTime {
        fn from(t: SystemTime) -> Self {
            // Minimal UTC conversion without external crates.
            // This is used only for a generated timestamp; it does not affect correctness.
            let secs = t.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
            unix_to_utc(secs)
        }
    }

    impl ToString for DateTime {
        fn to_string(&self) -> String {
            format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                self.y, self.m, self.d, self.hh, self.mm, self.ss
            )
        }
    }

    fn unix_to_utc(mut secs: i64) -> DateTime {
        // Algorithm: convert seconds since epoch to UTC date/time.
        // Source: well-known civil-from-days approach (Howard Hinnant).
        let ss = (secs.rem_euclid(60)) as u32;
        secs = secs.div_euclid(60);
        let mm = (secs.rem_euclid(60)) as u32;
        secs = secs.div_euclid(60);
        let hh = (secs.rem_euclid(24)) as u32;
        let days = secs.div_euclid(24);

        let (y, m, d) = civil_from_days(days);
        DateTime {
            y,
            m,
            d,
            hh,
            mm,
            ss,
        }
    }

    fn civil_from_days(z: i64) -> (i32, u32, u32) {
        // Convert days since 1970-01-01 to Gregorian date.
        let z = z + 719468;
        let era = (if z >= 0 { z } else { z - 146096 }).div_euclid(146097);
        let doe = z - era * 146097;
        let yoe = (doe - doe.div_euclid(1460) + doe.div_euclid(36524) - doe.div_euclid(146096))
            .div_euclid(365);
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe.div_euclid(4) - yoe.div_euclid(100));
        let mp = (5 * doy + 2).div_euclid(153);
        let d = doy - (153 * mp + 2).div_euclid(5) + 1;
        let m = mp + if mp < 10 { 3 } else { -9 };
        let y = y + if m <= 2 { 1 } else { 0 };
        (y as i32, m as u32, d as u32)
    }
}

fn main() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();
    let strict = args.iter().any(|a| a == "--strict");

    let repo_root = repo_root_from_args_or_cwd(&args)?;
    let out_path = parse_flag_value(&args, "--out")
        .map(PathBuf::from)
        .unwrap_or_else(|| repo_root.join("inventory.md"));

    let inventories = discover_component_inventories(&repo_root)?;
    let now = now_utc_iso_z();
    let repo_name = repo_root
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("repo");

    let mut all_file_paths: HashSet<String> = HashSet::new();
    let mut any_gap = false;

    let mut lines: Vec<String> = vec![];
    lines.push(format!(
        "# `{repo_name}` — Global Inventory (GENERATED; DO NOT EDIT)"
    ));
    lines.push("".to_string());
    lines.push(format!("Generated: {now}"));
    lines.push("Protocol: `docs/inventory_template.md`".to_string());
    lines.push("".to_string());
    lines.push(
        "This file is generated from per-component inventories under `*/src/docs/inventory.md` (preferred) and legacy `*/docs/inventory.md`."
            .to_string(),
    );
    lines.push(
        "If a file purpose is missing in a component inventory, this file will mark it as `INVENTORY GAP`."
            .to_string(),
    );
    lines.push("".to_string());
    lines.push("## Components".to_string());
    lines.push("".to_string());

    for inv in &inventories {
        let inv_rel = rel_path(&repo_root, &inv.inventory_path)?;
        validate_exists(&repo_root, &inv_rel)?;
        let kind_str = match inv.kind {
            ComponentKind::Crate => "crate",
            ComponentKind::Service => "service",
        };
        lines.push(format!("- `{kind_str}::{}`: `{inv_rel}`", inv.name));
    }

    lines.push("".to_string());
    lines.push("---".to_string());
    lines.push("".to_string());

    for inv in &inventories {
        let component_root = &inv.source_root;

        let artifacts = collect_artifacts(&repo_root, component_root, &inv.inventory_path)?;
        validate_exists(&repo_root, &artifacts.inventory_md)?;
        if let Some(scope) = &artifacts.scope_md {
            validate_exists(&repo_root, scope)?;
        }
        for s in &artifacts.spec_mds {
            validate_exists(&repo_root, s)?;
        }
        for e in &artifacts.evidence_logs {
            validate_exists(&repo_root, e)?;
        }
        for r in &artifacts.math_reviews {
            validate_exists(&repo_root, r)?;
        }
        for b in &artifacts.bench_logs {
            validate_exists(&repo_root, b)?;
        }
        if let Some(t) = &artifacts.tests_dir {
            validate_exists(&repo_root, t)?;
        }
        for b in &artifacts.benches {
            validate_exists(&repo_root, b)?;
        }

        let inv_text = fs::read_to_string(&inv.inventory_path)
            .map_err(|e| format!("read inventory failed: {:?}: {e}", inv.inventory_path))?;
        let raw_purposes = parse_inventory_source_file_purposes(&inv_text);
        let mut purposes: HashMap<String, String> = HashMap::new();
        let needs_crate_rebase = raw_purposes.keys().any(|p| {
            p.starts_with("src/")
                || p.starts_with("tests/")
                || p.starts_with("benches/")
                || p.starts_with("examples/")
                || p.starts_with("bin/")
        });
        let crate_root = if needs_crate_rebase {
            Some(find_crate_root(component_root)?)
        } else {
            None
        };
        for (path, purpose) in raw_purposes {
            let key = if path.starts_with("src/")
                || path.starts_with("tests/")
                || path.starts_with("benches/")
                || path.starts_with("examples/")
                || path.starts_with("bin/")
            {
                let Some(crate_root) = &crate_root else {
                    return Err(format!(
                        "inventory uses crate-local paths but crate root could not be determined: {}",
                        inv.inventory_path.display()
                    ));
                };
                let abs = crate_root.join(&path);
                rel_path(&repo_root, &abs)?
            } else {
                path
            };
            purposes.insert(key, purpose);
        }
        let component_files = component_source_files(&repo_root, component_root)?;

        let name = {
            let n = rel_path(&repo_root, component_root)?;
            if n.is_empty() {
                repo_name.to_string()
            } else {
                n
            }
        };

        lines.push(format!("## `{name}`"));
        lines.push("".to_string());
        lines.push("### Artifacts".to_string());
        lines.push("".to_string());
        lines.push(format!("- Inventory: `{}`", artifacts.inventory_md));
        if let Some(scope) = &artifacts.scope_md {
            lines.push(format!("- Scope: `{scope}`"));
        }
        for s in &artifacts.spec_mds {
            lines.push(format!("- Spec: `{s}`"));
        }
        for e in &artifacts.evidence_logs {
            lines.push(format!("- Evidence: `{e}`"));
        }
        for r in &artifacts.math_reviews {
            lines.push(format!("- Math review: `{r}`"));
        }
        for b in &artifacts.bench_logs {
            lines.push(format!("- Bench log: `{b}`"));
        }
        if let Some(t) = &artifacts.tests_dir {
            lines.push(format!("- Tests: `{t}`"));
        }
        for b in &artifacts.benches {
            lines.push(format!("- Benches: `{b}`"));
        }

        lines.push("".to_string());
        lines.push("### Source Files".to_string());
        lines.push("".to_string());

        for rel in component_files {
            if all_file_paths.contains(&rel) {
                return Err(format!("duplicate file path across modules: {rel}"));
            }
            all_file_paths.insert(rel.clone());

            if let Some(purpose) = purposes.get(&rel) {
                lines.push(format!("- `{rel}`: {purpose}"));
            } else {
                any_gap = true;
                lines.push(format!(
                    "- `{rel}`: INVENTORY GAP (add 1-line purpose in `{}`)",
                    artifacts.inventory_md
                ));
            }
        }

        lines.push("".to_string());
        lines.push("---".to_string());
        lines.push("".to_string());
    }

    let mut content = lines.join("\n");
    if !content.ends_with('\n') {
        content.push('\n');
    }
    fs::write(&out_path, content).map_err(|e| format!("write failed for {out_path:?}: {e}"))?;

    if strict && any_gap {
        std::process::exit(2);
    }
    Ok(())
}
