## [1.4.0](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/compare/v1.3.0...v1.4.0) (2026-05-07)

### Features

* expose gold layer via FastAPI REST endpoints with Swagger UI ([6abc511](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/6abc511f2c76e5ed5039e03d3d44439cebf22289))

## [1.3.0](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/compare/v1.2.0...v1.3.0) (2026-05-07)

### Features

* data quality validation with Great Expectations - 61 expectations all passed ([116acef](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/116acef407ad6bc9c2555a9e7da5725b224045eb))

### Bug Fixes

* remove unused imports and variables in data quality script ([29f1e0d](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/29f1e0df43629515c7c1f849acca0fae824bfd22))

## [1.2.0](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/compare/v1.1.0...v1.2.0) (2026-05-07)

### Features

* gold layer with 4 analytical tables and unit tests ([f810a4a](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/f810a4a187376d2aa8a38f827ad7746ae8a45759))
* gold layer with conformite, parametres risks, commune stats and evolution mensuelle ([da0df20](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/da0df203bffd1568a29fd58a8183b11ad2c1984d))

### Bug Fixes

* remove unused imports in gold transform and tests ([55f84f1](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/55f84f1d40126977ac94f168effbba10b8627981))
* skip Spark tests in CI due to Maven network restrictions ([f4e560e](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/f4e560ec76edecd4c01da886f074043aa9c4fc94))
* wrap gold notebook execution in _NOTEBOOK_RUN guard for testability ([cb69826](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/cb69826b9d2a905864e12e830537b9c75941a4bd))

## [1.1.0](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/compare/v1.0.1...v1.1.0) (2026-05-06)

### Features

* **silver:** pipeline Silver complet — nettoyage, enrichissement géo, catégorisation, conformité, tests unitaires ([34a87fc](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/34a87fc08f97bb2adf23cb6e98541e8fc42a9da7))

### Bug Fixes

* **tests:** remove undefined DataFrame type hint in make_water_df ([5cd7337](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/5cd73378e399e57ccd30ccbe46e562b4a719491a))

## [1.0.1](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/compare/v1.0.0...v1.0.1) (2026-05-06)

### Bug Fixes

* refactor bronze scripts for testability and flake8 compliance ([cd13af2](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/cd13af2de6544c2cbf94e4b1e819f30c94bac660))
* resolve merge conflict in ingestion_geo.py ([a1f6d99](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/a1f6d99cc601a19f6773c6122dba4492b3bb8b25))

## 1.0.0 (2026-05-06)

### Features

* add CI/CD with GitHub Actions,semantic release ([64d8e8b](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/64d8e8b888ccb8a5b6939d6f20de66cf7862a3eb))
* add parallel download with MD5 check and HTTP range resume ([ad10aca](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/ad10aca1be4b069e843a9dae7b58417af088dbd0))
* bronze ingestion Hub'Eau API with 4-level pagination ([5df92a2](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/5df92a2ec2fecaf679401922536e793a6599daa6))
* bronze ingestion with dlt for water results, geo data & config.yaml ([b0b963b](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/b0b963bd2c5f6d6ba04aceeaff4203c34b47c364))

### Bug Fixes

* add __init__.py and configure pythonpath for pytest ([4862d8d](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/4862d8de407881c7d49b2100681f68a13bb5ebdd))
* fix flake8 E203 and W292 in ingestion_hubeau ([d7259d3](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/d7259d3a244cebea383d74d8ed026f0654482cef))
* fix flake8 erreos in bronze notebooks ([570ba86](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/570ba86ba420943a5f1d5aa2b3781f68c74642e0))
* set working directory in CI workflow ([93af514](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/93af51442006a98815ae95b6f44cebfa8d6dd41e))
* wrap pipeline execution in __main__ guard ([ef95988](https://github.com/Simplon-DE-P1-2025/brief-water-quality-pipeline/commit/ef95988b1af2bb5ce2c6718dabb2c64404d93aac))
