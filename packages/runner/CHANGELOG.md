# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.5](https://github.com/8xFF/atm0s-sdn/compare/atm0s-sdn-v0.2.4...atm0s-sdn-v0.2.5) - 2024-11-29

### Fixed

- avoid send wrong hops with router-sync local services ([#184](https://github.com/8xFF/atm0s-sdn/pull/184))

## [0.2.4](https://github.com/8xFF/atm0s-sdn/compare/atm0s-sdn-v0.2.3...atm0s-sdn-v0.2.4) - 2024-11-26

### Added

- add router dump ([#182](https://github.com/8xFF/atm0s-sdn/pull/182))

## [0.2.3](https://github.com/8xFF/atm0s-sdn/compare/atm0s-sdn-v0.2.2...atm0s-sdn-v0.2.3) - 2024-11-08

### Added

- task clean-up with new sans-io-runtime ([#179](https://github.com/8xFF/atm0s-sdn/pull/179))

### Other

- update Cargo.toml dependencies

## [0.2.2](https://github.com/8xFF/atm0s-sdn/compare/atm0s-sdn-v0.2.1...atm0s-sdn-v0.2.2) - 2024-07-22

### Added
- listen multiple UDP addresses ([#175](https://github.com/8xFF/atm0s-sdn/pull/175))

## [0.2.1](https://github.com/8xFF/atm0s-sdn/compare/atm0s-sdn-v0.2.0...atm0s-sdn-v0.2.1) - 2024-07-22

### Fixed
- switch to single bind for resolve connect error with multi-addresses machine ([#172](https://github.com/8xFF/atm0s-sdn/pull/172))

### Other
- clippy fixes ([#167](https://github.com/8xFF/atm0s-sdn/pull/167))

## [0.1.10](https://github.com/8xFF/atm0s-sdn/compare/atm0s-sdn-v0.1.9...atm0s-sdn-v0.1.10) - 2024-02-21

### Other
- updated the following local packages: atm0s-sdn-virtual-socket

## [0.1.9](https://github.com/8xFF/atm0s-sdn/compare/atm0s-sdn-v0.1.8...atm0s-sdn-v0.1.9) - 2024-01-31

### Fixed
- key value service resub failed if run before unsub ack and subscribe slot is removed ([#123](https://github.com/8xFF/atm0s-sdn/pull/123))

### Other
- update rust crate snow to 0.9.6 ([#121](https://github.com/8xFF/atm0s-sdn/pull/121))

## [0.1.8](https://github.com/8xFF/atm0s-sdn/compare/atm0s-sdn-v0.1.7...atm0s-sdn-v0.1.8) - 2024-01-24

### Fixed
- *(deps)* update rust crate snow to 0.9.5 ([#120](https://github.com/8xFF/atm0s-sdn/pull/120))
- *(deps)* update rust crate local-ip-address to 0.5.7 ([#117](https://github.com/8xFF/atm0s-sdn/pull/117))

### Other
- update Cargo.toml dependencies

## [0.1.7](https://github.com/8xFF/atm0s-sdn/compare/atm0s-sdn-v0.1.6...atm0s-sdn-v0.1.7) - 2024-01-09

### Added
- node_alias service ([#110](https://github.com/8xFF/atm0s-sdn/pull/110))
- virtual udp socket and quinn for tunneling between two nodes ([#107](https://github.com/8xFF/atm0s-sdn/pull/107))

### Fixed
- send node-alias response more faster ([#111](https://github.com/8xFF/atm0s-sdn/pull/111))

## [0.1.6](https://github.com/8xFF/atm0s-sdn/compare/atm0s-sdn-v0.1.5...atm0s-sdn-v0.1.6) - 2023-12-28

### Fixed
- fix key-value fired duplicated events in case of multi subscribers ([#104](https://github.com/8xFF/atm0s-sdn/pull/104))

## [0.1.5](https://github.com/8xFF/atm0s-sdn/compare/atm0s-sdn-v0.1.4...atm0s-sdn-v0.1.5) - 2023-12-27

### Added
- secure with static key and noise protocol ([#101](https://github.com/8xFF/atm0s-sdn/pull/101))
- node multi addrs ([#98](https://github.com/8xFF/atm0s-sdn/pull/98))

### Other
- update Cargo.toml dependencies

## [0.1.4](https://github.com/8xFF/atm0s-sdn/compare/atm0s-sdn-v0.1.3...atm0s-sdn-v0.1.4) - 2023-12-12

### Other
- update dependencies

## [0.1.3](https://github.com/8xFF/atm0s-sdn/compare/atm0s-sdn-v0.1.2...atm0s-sdn-v0.1.3) - 2023-12-11

### Other
- move local deps out of workspace Cargo.toml ([#92](https://github.com/8xFF/atm0s-sdn/pull/92))

## [0.1.2](https://github.com/8xFF/atm0s-sdn/compare/atm0s-sdn-v0.1.1...atm0s-sdn-v0.1.2) - 2023-12-11

### Added
- rpc service and fi ([#87](https://github.com/8xFF/atm0s-sdn/pull/87))
- manual-discovery with node tags ([#84](https://github.com/8xFF/atm0s-sdn/pull/84))

### Fixed
- missing register service ([#85](https://github.com/8xFF/atm0s-sdn/pull/85))
- *(deps)* update rust crate url to 2.5.0 ([#74](https://github.com/8xFF/atm0s-sdn/pull/74))
- *(deps)* update rust crate percent-encoding to 2.3.1 ([#73](https://github.com/8xFF/atm0s-sdn/pull/73))
- *(deps)* update rust crate data-encoding to 2.5 ([#72](https://github.com/8xFF/atm0s-sdn/pull/72))
- router sync service handler missing pop actions ([#82](https://github.com/8xFF/atm0s-sdn/pull/82))

## [0.1.1](https://github.com/8xFF/atm0s-sdn/compare/atm0s-sdn-v0.1.0...atm0s-sdn-v0.1.1) - 2023-11-23

### Fixed
- *(deps)* update rust crate serde to 1.0.193 ([#71](https://github.com/8xFF/atm0s-sdn/pull/71))
- key-value event not fired with second subs ([#79](https://github.com/8xFF/atm0s-sdn/pull/79))
