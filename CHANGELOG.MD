# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [2.10]

### Added

- Added native association support using ConnId object references, including reference-aware schema generation, search, and synchronization.
- Added creation and modification support for simple associations, including membership stored in a local `memberOf` attribute.
- Added referential integrity handling for configured associations.
- Added configuration properties `managedAssociationPairs`, `assocAttrDirect`, and `assocAttrIndirect` to define associations and customize reference attribute names.
- Added synchronization across object classes using `ObjectClass.ALL` when managed associations are configured, with processing ordered according to association relationships.

### Changed

- Improved the ordering, labels, and descriptions of connector configuration properties.
- Improved automatic discovery of identifier columns by excluding columns with duplicate or missing values in the first 1,000 rows, when CSV parsing parameters can be determined.
- Updated the connector parent from `1.5.2.0` to `1.5.3.0` and added an explicit Apache Commons Lang dependency at version `3.20.0`.
- Updated the Dependency-Track Maven plugin from `1.7.0` to `1.10.2`.

### Fixed

- Fixed error handling for empty unique attribute values (MID-10911).
- Fixed false comment-marker detection during configuration discovery, including cases where ordinary CSV values begin with punctuation (MID-10956).
- Fixed unintended propagation of unique attribute configuration from the top-level configuration and improved identification of unique attributes in referenced objects.
- Improved identification of referenced objects using `Name.NAME` and handling of create and update operations whose attribute sets do not contain a UID.

### Known limitations

- Complex associations (e.g. User - Access - Group) are supported for reading only, creating them is not supported.