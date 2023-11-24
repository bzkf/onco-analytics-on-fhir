# Changelog

## [2.1.1](https://github.com/bzkf/diz-in-a-box/compare/v2.1.0...v2.1.1) (2023-11-24)


### Bug Fixes

* update connector SQL query and related part in README.md ([#126](https://github.com/bzkf/diz-in-a-box/issues/126)) ([4332970](https://github.com/bzkf/diz-in-a-box/commit/4332970d278c274267871c7199f6f95f93f64c76))


### Miscellaneous Chores

* **deps:** updated pseudonymizer to v2.21.3 ([#128](https://github.com/bzkf/diz-in-a-box/issues/128)) ([41dbbc2](https://github.com/bzkf/diz-in-a-box/commit/41dbbc25d4c06ed14e7b4774334b8290892050c0))

## [2.1.0](https://github.com/bzkf/diz-in-a-box/compare/v2.0.0...v2.1.0) (2023-11-21)


### Features

* age and entity groupings for datashield ([#124](https://github.com/bzkf/diz-in-a-box/issues/124)) ([c03d2d3](https://github.com/bzkf/diz-in-a-box/commit/c03d2d318c18ad2e59003fe792e61d39751c0528))


### Miscellaneous Chores

* bumps obds-to-fhir ([#120](https://github.com/bzkf/diz-in-a-box/issues/120)) ([e3035dd](https://github.com/bzkf/diz-in-a-box/commit/e3035dd4b0104946970d50c57f6e901638ca2c01))
* **deps:** update github/codeql-action action to v2.22.6 ([#122](https://github.com/bzkf/diz-in-a-box/issues/122)) ([e1fef29](https://github.com/bzkf/diz-in-a-box/commit/e1fef29fb0d5b3a13387458fd7e480155d137a31))


### CI/CD

* use reusable miracum workflows for ci ([#123](https://github.com/bzkf/diz-in-a-box/issues/123)) ([ff86902](https://github.com/bzkf/diz-in-a-box/commit/ff869028c41ef5c2cb85b2fd2a52896b6de087d3))

## [2.0.0](https://github.com/bzkf/diz-in-a-box/compare/v1.6.2...v2.0.0) (2023-11-08)


### ⚠ BREAKING CHANGES

* affects (default) names of all created and used Kafka topics

### Features

* renamed `adt` to `obds` ([581311b](https://github.com/bzkf/diz-in-a-box/commit/581311b27644c6e41e09e5879fb2c82b153eea9a))
* renamed adt to obds - BREAKING CHANGE - renames all container images ([9300032](https://github.com/bzkf/diz-in-a-box/commit/9300032f28f92b24e4e6acc9fb31a3c50b8a1468))


### Miscellaneous Chores

* bumps obds-to-fhir ([7be7d17](https://github.com/bzkf/diz-in-a-box/commit/7be7d174037719742adf9ee47f4cad48adea0b50))
* bumps obds-to-fhir images ([a1a98ac](https://github.com/bzkf/diz-in-a-box/commit/a1a98accddc7f3d4a8470e6d0ba4e97d79350957))
* make megalinter black happy ([3c6946c](https://github.com/bzkf/diz-in-a-box/commit/3c6946ceace2065984d3f1329e09f6a0cbb2a6f8))
* markdownlint ([8a59ef7](https://github.com/bzkf/diz-in-a-box/commit/8a59ef73eba16518183ee18bb37da1a6da98fd9e))
* updates obds-to-fhir image registry ([#118](https://github.com/bzkf/diz-in-a-box/issues/118)) ([ed3398e](https://github.com/bzkf/diz-in-a-box/commit/ed3398ee37744e91c6de541b17490275df68bc6d))

## [1.6.2](https://github.com/bzkf/diz-in-a-box/compare/v1.6.1...v1.6.2) (2023-10-12)


### Miscellaneous Chores

* **deps:** downgraded helm to create release ([#113](https://github.com/bzkf/diz-in-a-box/issues/113)) ([657bd67](https://github.com/bzkf/diz-in-a-box/commit/657bd678d925d941de0867706b68b2f88751d574))

## [1.6.1](https://github.com/bzkf/diz-in-a-box/compare/v1.6.0...v1.6.1) (2023-10-12)


### Miscellaneous Chores

* **deps:** updated fhir pseudonymizer and gateway to latest ([#111](https://github.com/bzkf/diz-in-a-box/issues/111)) ([3925bcc](https://github.com/bzkf/diz-in-a-box/commit/3925bcccf3a556e15dbb04f9a835f44472476986))

## [1.6.0](https://github.com/bzkf/diz-in-a-box/compare/v1.5.11...v1.6.0) (2023-10-04)


### Features

* added a sample deployment for datashield/opal ([#105](https://github.com/bzkf/diz-in-a-box/issues/105)) ([d18fa12](https://github.com/bzkf/diz-in-a-box/commit/d18fa120a7cf2a68bca6ba25c0900ee2ab2f9087))
* updated adtfhir-to-opal to produce uicc mapping, grouped uicc, age at diagnosis ([#107](https://github.com/bzkf/diz-in-a-box/issues/107)) ([3d223e8](https://github.com/bzkf/diz-in-a-box/commit/3d223e81003a38e51d821a8eee5d8ac38ba4e558))


### Miscellaneous Chores

* added incomplete taskfile for demoing ([0764e7a](https://github.com/bzkf/diz-in-a-box/commit/0764e7af4c2ad56820aa21c4075ba2c0f711f4f7))
* **deps:** update actions/checkout action to v4 ([#106](https://github.com/bzkf/diz-in-a-box/issues/106)) ([09a8064](https://github.com/bzkf/diz-in-a-box/commit/09a806430b0c2d7b2163b6adf94433b3e94c276e))
* **deps:** update compose.onkoadt-to-fhir.yaml to use harbor image variant ([#109](https://github.com/bzkf/diz-in-a-box/issues/109)) ([9b546c0](https://github.com/bzkf/diz-in-a-box/commit/9b546c0ccf5fdb58626feacbfd291896ba82ea92))
* **deps:** update ghcr.io/miracum/kafka-fhir-to-server docker tag to v2 ([#96](https://github.com/bzkf/diz-in-a-box/issues/96)) ([032906b](https://github.com/bzkf/diz-in-a-box/commit/032906bf15e15bc7b095b969a3664a4c3c6c4e75))
* **deps:** update github-actions ([#31](https://github.com/bzkf/diz-in-a-box/issues/31)) ([51782af](https://github.com/bzkf/diz-in-a-box/commit/51782afc4b69d5a25ce2fb7a3053e9c35235a892))
* **deps:** update github-actions (major) ([#60](https://github.com/bzkf/diz-in-a-box/issues/60)) ([26c1666](https://github.com/bzkf/diz-in-a-box/commit/26c166642194aa11eeea5809e80c7e9830937f12))
* **deps:** update google-github-actions/release-please-action action to v3.7.12 ([#108](https://github.com/bzkf/diz-in-a-box/issues/108)) ([edeabf6](https://github.com/bzkf/diz-in-a-box/commit/edeabf65441aa70abd2a1e37ac9abc18f574d269))
* fixed KRaft config for newest Kafka ([#101](https://github.com/bzkf/diz-in-a-box/issues/101)) ([84df54e](https://github.com/bzkf/diz-in-a-box/commit/84df54e4d8b31256781d948fc6e71721d8d44c94))
* unpin strimzi Operator Image for easier updates ([f13faa8](https://github.com/bzkf/diz-in-a-box/commit/f13faa8bf20aa3d5a9ceebdbfa59cbb37ef8d572))
* updated FHIR Pseudonymizer to v2.20.0 ([#104](https://github.com/bzkf/diz-in-a-box/issues/104)) ([66897d4](https://github.com/bzkf/diz-in-a-box/commit/66897d40e910c7b7963c86e18d1c984ea3898a66))
* use a smaller spark base image ([#103](https://github.com/bzkf/diz-in-a-box/issues/103)) ([862e124](https://github.com/bzkf/diz-in-a-box/commit/862e12457c17a7ebd6e09015661b16b16a6db177))

## [1.5.11](https://github.com/bzkf/diz-in-a-box/compare/v1.5.10...v1.5.11) (2023-08-03)


### Miscellaneous Chores

* **deps:** update container-images ([#98](https://github.com/bzkf/diz-in-a-box/issues/98)) ([e2628f1](https://github.com/bzkf/diz-in-a-box/commit/e2628f143d25a05d762d97218659d04fc93eb74b))

## [1.5.10](https://github.com/bzkf/diz-in-a-box/compare/v1.5.9...v1.5.10) (2023-07-10)


### Bug Fixes

* set kafka cleanup policy to compact ([#92](https://github.com/bzkf/diz-in-a-box/issues/92)) ([531ff40](https://github.com/bzkf/diz-in-a-box/commit/531ff40d0cb8a56417806fb66d1055a9eadaca86))

## [1.5.9](https://github.com/bzkf/diz-in-a-box/compare/v1.5.8...v1.5.9) (2023-07-09)


### Bug Fixes

* restart policy and output volume ([16bd28d](https://github.com/bzkf/diz-in-a-box/commit/16bd28de1d2591f28314efd09913ab38c3fc8a0f))


### Miscellaneous Chores

* **deps:** update all non-major dependencies ([#89](https://github.com/bzkf/diz-in-a-box/issues/89)) ([648b6f5](https://github.com/bzkf/diz-in-a-box/commit/648b6f50dce4852079ec830466c672db04725190))
* **deps:** update ghcr.io/miracum/kafka-fhir-to-server docker tag to v1.2.7 ([#90](https://github.com/bzkf/diz-in-a-box/issues/90)) ([6d3a476](https://github.com/bzkf/diz-in-a-box/commit/6d3a47680343a8e7e985804f77fcaecabfc82ac8))

## [1.5.8](https://github.com/bzkf/diz-in-a-box/compare/v1.5.7...v1.5.8) (2023-07-06)


### Miscellaneous Chores

* **deps:** update all non-major dependencies ([#85](https://github.com/bzkf/diz-in-a-box/issues/85)) ([8c22423](https://github.com/bzkf/diz-in-a-box/commit/8c2242357ed54e940996e46d9c186a655f43a45a))
* **deps:** update docker.io/library/python:3.11.4-slim docker digest to 364ee1a ([#87](https://github.com/bzkf/diz-in-a-box/issues/87)) ([f9b6946](https://github.com/bzkf/diz-in-a-box/commit/f9b6946820cf628ef6f801687b4ebe8879c93ae1))
* **deps:** update kafka-connect-image to v1.1.0 ([af0a423](https://github.com/bzkf/diz-in-a-box/commit/af0a42392a9d0525f46671331af890b2e68b057d))


### CI/CD

* release-please extra file fix ([6f6e611](https://github.com/bzkf/diz-in-a-box/commit/6f6e611a52677204d2a76c656012b80d7d01979b))

## [1.5.7](https://github.com/bzkf/diz-in-a-box/compare/v1.5.6...v1.5.7) (2023-06-27)


### Miscellaneous Chores

* **deps:** update all non-major dependencies ([#81](https://github.com/bzkf/diz-in-a-box/issues/81)) ([1b56665](https://github.com/bzkf/diz-in-a-box/commit/1b56665e5e72ce86351c53c884891786401e0e46))
* **deps:** update container-images ([#82](https://github.com/bzkf/diz-in-a-box/issues/82)) ([16dc67c](https://github.com/bzkf/diz-in-a-box/commit/16dc67c1450ef45ce1364a954016915a19ab6a11))

## [1.5.6](https://github.com/bzkf/diz-in-a-box/compare/v1.5.5...v1.5.6) (2023-06-20)


### Bug Fixes

* depend only on build for releases ([1252348](https://github.com/bzkf/diz-in-a-box/commit/1252348a380bc76a27f5d44451ad82c629f63882))

## [1.5.5](https://github.com/bzkf/diz-in-a-box/compare/v1.5.4...v1.5.5) (2023-06-20)


### Bug Fixes

* re-structured compose files ([1f9f591](https://github.com/bzkf/diz-in-a-box/commit/1f9f591e39cb01a32a6f2f4a0e9fc865f3b522a9))


### CI/CD

* run tests only on PRs to keep the pipeline fast ([a84d656](https://github.com/bzkf/diz-in-a-box/commit/a84d656884d40e5633a5a1a2a544a749a35d31ea))

## [1.5.4](https://github.com/bzkf/diz-in-a-box/compare/v1.5.3...v1.5.4) (2023-06-20)


### Miscellaneous Chores

* **deps:** update all non-major dependencies ([#73](https://github.com/bzkf/diz-in-a-box/issues/73)) ([f070ed9](https://github.com/bzkf/diz-in-a-box/commit/f070ed9cc27bac3be5d8fa0745a9caec170b999a))
* **deps:** update ghcr.io/miracum/kafka-fhir-to-server docker tag to v1.2.6 ([#78](https://github.com/bzkf/diz-in-a-box/issues/78)) ([1a9c7bd](https://github.com/bzkf/diz-in-a-box/commit/1a9c7bda1123de276f1dab4cd9515aa7109f4ef2))

## [1.5.3](https://github.com/bzkf/diz-in-a-box/compare/v1.5.2...v1.5.3) (2023-06-13)


### Miscellaneous Chores

* **deps:** update container-images ([#71](https://github.com/bzkf/diz-in-a-box/issues/71)) ([b1a7f81](https://github.com/bzkf/diz-in-a-box/commit/b1a7f81c106470c26d43d2cdc27b7751aa5378b2))
* **deps:** update docker.io/library/python:3.11.4-slim docker digest to 0f0fca2 ([#75](https://github.com/bzkf/diz-in-a-box/issues/75)) ([30b086b](https://github.com/bzkf/diz-in-a-box/commit/30b086b37ea993652a4579293863c6de83d48247))

## [1.5.2](https://github.com/bzkf/diz-in-a-box/compare/v1.5.1...v1.5.2) (2023-06-07)


### Bug Fixes

* add missing payload ([#70](https://github.com/bzkf/diz-in-a-box/issues/70)) ([de0c7fd](https://github.com/bzkf/diz-in-a-box/commit/de0c7fd63310e86b2a89194292cc9311c1ce9fdb))

## [1.5.1](https://github.com/bzkf/diz-in-a-box/compare/v1.5.0...v1.5.1) (2023-06-05)


### CI/CD

* use custom cricketeer image and only build compose installer ([#67](https://github.com/bzkf/diz-in-a-box/issues/67)) ([cbe90d9](https://github.com/bzkf/diz-in-a-box/commit/cbe90d9cda282e70988f5f3c1dcaaa348194e51e))

## [1.5.0](https://github.com/bzkf/diz-in-a-box/compare/v1.4.2...v1.5.0) (2023-06-04)


### Features

* added input data preprocessor to decompose sammelmeldung xmls ([#38](https://github.com/bzkf/diz-in-a-box/issues/38)) ([ef3a73e](https://github.com/bzkf/diz-in-a-box/commit/ef3a73e8ec0296ced92d710efa5e13c5679ddeff))
* include decompose xml in compose installation ([#66](https://github.com/bzkf/diz-in-a-box/issues/66)) ([52c53b2](https://github.com/bzkf/diz-in-a-box/commit/52c53b2281d5c36c74964d5039bf312a7a905256))


### Miscellaneous Chores

* **deps:** update container-images ([#61](https://github.com/bzkf/diz-in-a-box/issues/61)) ([f125e2f](https://github.com/bzkf/diz-in-a-box/commit/f125e2fd594765fb497f8b25a394edda234af252))
* **deps:** update ghcr.io/miracum/kafka-fhir-to-server docker tag to v1.2.4 ([#63](https://github.com/bzkf/diz-in-a-box/issues/63)) ([81047f7](https://github.com/bzkf/diz-in-a-box/commit/81047f7996acfb9238d244414ef6bfbb6511b3b6))
* **deps:** update ghcr.io/miracum/kafka-fhir-to-server docker tag to v1.2.5 ([#64](https://github.com/bzkf/diz-in-a-box/issues/64)) ([1d34c23](https://github.com/bzkf/diz-in-a-box/commit/1d34c23b96875ce29d10c67526d5e1d05f2f820f))

## [1.4.2](https://github.com/bzkf/diz-in-a-box/compare/v1.4.1...v1.4.2) (2023-05-24)


### Bug Fixes

* use ghcr.io registry for onkoadt-to-fhir ([#58](https://github.com/bzkf/diz-in-a-box/issues/58)) ([a628a62](https://github.com/bzkf/diz-in-a-box/commit/a628a62cd294c64d3feb8407c0d9f56277ed8c63))

## [1.4.1](https://github.com/bzkf/diz-in-a-box/compare/v1.4.0...v1.4.1) (2023-05-23)


### Miscellaneous Chores

* **deps:** update all non-major dependencies ([#55](https://github.com/bzkf/diz-in-a-box/issues/55)) ([bb157b4](https://github.com/bzkf/diz-in-a-box/commit/bb157b4ce5937cd7944fb8651ef0579ebf836ab2))
* **deps:** update container-images ([#54](https://github.com/bzkf/diz-in-a-box/issues/54)) ([f0d70a4](https://github.com/bzkf/diz-in-a-box/commit/f0d70a419c13bf132cd57311be8213de6ff747e3))
* **deps:** update harbor.miracum.org/streams-ume/onkoadt-to-fhir docker tag to v1.10.4 ([#57](https://github.com/bzkf/diz-in-a-box/issues/57)) ([e4f4e0f](https://github.com/bzkf/diz-in-a-box/commit/e4f4e0f9919666b3dbd29b8edeef102cd78f1ca5))

## [1.4.0](https://github.com/bzkf/diz-in-a-box/compare/v1.3.1...v1.4.0) (2023-05-08)


### Features

* added onkostar db connector ([787b21e](https://github.com/bzkf/diz-in-a-box/commit/787b21ef6f7481d8797f50c61fe5d928501e96e5))


### Bug Fixes

* anonymization to work with multiple identifier coding types ([787b21e](https://github.com/bzkf/diz-in-a-box/commit/787b21ef6f7481d8797f50c61fe5d928501e96e5))
* don't pin compose digests ([#53](https://github.com/bzkf/diz-in-a-box/issues/53)) ([787b21e](https://github.com/bzkf/diz-in-a-box/commit/787b21ef6f7481d8797f50c61fe5d928501e96e5))
* un-pin compose images due to missing repo names when docker-saving ([787b21e](https://github.com/bzkf/diz-in-a-box/commit/787b21ef6f7481d8797f50c61fe5d928501e96e5))


### Miscellaneous Chores

* adds patient topic ([#51](https://github.com/bzkf/diz-in-a-box/issues/51)) ([c3c4328](https://github.com/bzkf/diz-in-a-box/commit/c3c43286ab1ffe47c1ca9ce30ce8bcbede0a0447))

## [1.3.1](https://github.com/bzkf/diz-in-a-box/compare/v1.3.0...v1.3.1) (2023-05-08)


### Bug Fixes

* split artifacts into multiple to deal with file sizes ([0edbcf2](https://github.com/bzkf/diz-in-a-box/commit/0edbcf29e17427f34b1e6e6e4b5f08dfd8e04952))

## [1.3.0](https://github.com/bzkf/diz-in-a-box/compare/v1.2.0...v1.3.0) (2023-05-07)


### Features

* added compose-based deployment and k3s image import script ([#47](https://github.com/bzkf/diz-in-a-box/issues/47)) ([1b69d27](https://github.com/bzkf/diz-in-a-box/commit/1b69d27f859b6e1606e5e2fe1c23ef55cb9ad990))


### Bug Fixes

* not iterating over all images to download the air-gapped images ([#45](https://github.com/bzkf/diz-in-a-box/issues/45)) ([35f46c1](https://github.com/bzkf/diz-in-a-box/commit/35f46c17543096c701e687cabb5f1799ee352bb5))
* use digests consistently ([#48](https://github.com/bzkf/diz-in-a-box/issues/48)) ([6ceee8d](https://github.com/bzkf/diz-in-a-box/commit/6ceee8d7bf950bdd03b572a99b875623777cc0a4))


### Miscellaneous Chores

* **deps:** update container-images ([#42](https://github.com/bzkf/diz-in-a-box/issues/42)) ([6a88ad6](https://github.com/bzkf/diz-in-a-box/commit/6a88ad6d472c8795db67617dd2708b5c55a6d5df))
* **deps:** update container-images ([#49](https://github.com/bzkf/diz-in-a-box/issues/49)) ([74c02fd](https://github.com/bzkf/diz-in-a-box/commit/74c02fdcf5cefe5daacb78ac06862241eef0a893))
* **deps:** update harbor.miracum.org/streams-ume/onkoadt-to-fhir docker tag to v1.10.0 ([#46](https://github.com/bzkf/diz-in-a-box/issues/46)) ([2fc9707](https://github.com/bzkf/diz-in-a-box/commit/2fc9707283ee015d6b94a672398b71416b4cbc52))
* **deps:** update harbor.miracum.org/streams-ume/onkoadt-to-fhir docker tag to v1.9.0 ([#43](https://github.com/bzkf/diz-in-a-box/issues/43)) ([dc1f1c8](https://github.com/bzkf/diz-in-a-box/commit/dc1f1c8822b3d3165eae910db3d631b9e9224a6f))

## [1.2.0](https://github.com/bzkf/diz-in-a-box/compare/v1.1.2...v1.2.0) (2023-05-04)


### Features

* added docker-compose based installation method ([#39](https://github.com/bzkf/diz-in-a-box/issues/39)) ([1227a45](https://github.com/bzkf/diz-in-a-box/commit/1227a453f6c297461ae6826a76f6353f4e520f2e))


### Miscellaneous Chores

* updated fhir gw to update pseudonymizer ([#41](https://github.com/bzkf/diz-in-a-box/issues/41)) ([c5c22d5](https://github.com/bzkf/diz-in-a-box/commit/c5c22d555f524c320361e3a55beff098bc54e82f))

## [1.1.2](https://github.com/bzkf/diz-in-a-box/compare/v1.1.1...v1.1.2) (2023-05-02)


### Bug Fixes

* default to partition count of 1 and gzip compression ([#35](https://github.com/bzkf/diz-in-a-box/issues/35)) ([4574d7a](https://github.com/bzkf/diz-in-a-box/commit/4574d7abae4051b8eb379525baf07edf6829ec7b))


### Miscellaneous Chores

* **deps:** update harbor.miracum.org/streams-ume/onkoadt-to-fhir docker tag to v1.8.1 ([#37](https://github.com/bzkf/diz-in-a-box/issues/37)) ([c9aa56b](https://github.com/bzkf/diz-in-a-box/commit/c9aa56be204750d6cffbf78d46311eae1cf98748))

## [1.1.1](https://github.com/bzkf/diz-in-a-box/compare/v1.1.0...v1.1.1) (2023-04-19)


### Bug Fixes

* added prerequisites chart to release-please extra-files ([bdf2146](https://github.com/bzkf/diz-in-a-box/commit/bdf2146c463d89effbf67c9acfd3944ba6a423e5))

## [1.1.0](https://github.com/bzkf/diz-in-a-box/compare/v1.0.0...v1.1.0) (2023-04-19)


### Features

* added air-gapped installer workflow ([#33](https://github.com/bzkf/diz-in-a-box/issues/33)) ([32aaaf2](https://github.com/bzkf/diz-in-a-box/commit/32aaaf26653f0b710ccc69397ac46983950f7e04))
* added kafka bridge deployment ([#29](https://github.com/bzkf/diz-in-a-box/issues/29)) ([6bb2feb](https://github.com/bzkf/diz-in-a-box/commit/6bb2feb2979e11735e5ec6149d9a6f49d47a073e))
* added support for deploying KafkaBridge ([6bb2feb](https://github.com/bzkf/diz-in-a-box/commit/6bb2feb2979e11735e5ec6149d9a6f49d47a073e))
* include fhir-gateway sub-chart and config to provide pseudonymization ([#16](https://github.com/bzkf/diz-in-a-box/issues/16)) ([8e9a503](https://github.com/bzkf/diz-in-a-box/commit/8e9a503514420929528e925286fd0158a6f04d8f))


### Documentation

* use OCI chart in README and update version via release please ([385708f](https://github.com/bzkf/diz-in-a-box/commit/385708fa08aa153173cddd5850b6dac9f4404893))


### CI/CD

* fix megalinter complaints and added scorecards workflow ([#10](https://github.com/bzkf/diz-in-a-box/issues/10)) ([1be905e](https://github.com/bzkf/diz-in-a-box/commit/1be905e1d02e04940cb00628fdcb6249af1ae133))
* fixed cosign ([#12](https://github.com/bzkf/diz-in-a-box/issues/12)) ([2774c4f](https://github.com/bzkf/diz-in-a-box/commit/2774c4fd81c3a1f80b5e91d690b523c295dae815))


### Miscellaneous Chores

* add renovate.json ([#4](https://github.com/bzkf/diz-in-a-box/issues/4)) ([410f49b](https://github.com/bzkf/diz-in-a-box/commit/410f49b67ac3542900c29e93785ff0d5134c25b1))
* **deps:** update actions/checkout action to v3.4.0 ([#9](https://github.com/bzkf/diz-in-a-box/issues/9)) ([0ab830d](https://github.com/bzkf/diz-in-a-box/commit/0ab830d9bc212e07595d3200ee95128e3bab7837))
* **deps:** update actions/checkout action to v3.5.0 ([#14](https://github.com/bzkf/diz-in-a-box/issues/14)) ([04d8413](https://github.com/bzkf/diz-in-a-box/commit/04d8413a55dc1e91c4d722acf50dc70f35e3b92e))
* **deps:** update actions/checkout action to v3.5.2 ([#27](https://github.com/bzkf/diz-in-a-box/issues/27)) ([86972d5](https://github.com/bzkf/diz-in-a-box/commit/86972d5ff7694d2ef5d87a2c128e846e4d361f50))
* **deps:** update github/codeql-action action to v2.2.7 ([#11](https://github.com/bzkf/diz-in-a-box/issues/11)) ([e920199](https://github.com/bzkf/diz-in-a-box/commit/e920199a6d2f41184b6e6046bbd548d14a646639))
* **deps:** update github/codeql-action action to v2.2.8 ([#13](https://github.com/bzkf/diz-in-a-box/issues/13)) ([319b2e8](https://github.com/bzkf/diz-in-a-box/commit/319b2e8d4f51714ad9d674809e9f7ac1dcd81e9c))
* **deps:** update github/codeql-action action to v2.2.9 ([#17](https://github.com/bzkf/diz-in-a-box/issues/17)) ([a11a505](https://github.com/bzkf/diz-in-a-box/commit/a11a505aee6fdab65567eaf2a7ddb727ed7497d5))
* **deps:** update google-github-actions/release-please-action action to v3.7.6 ([#19](https://github.com/bzkf/diz-in-a-box/issues/19)) ([673372e](https://github.com/bzkf/diz-in-a-box/commit/673372e1a4a4d9bd1f49fa9a7ed2f676ad36dacd))
* **deps:** update google-github-actions/release-please-action action to v3.7.7 ([#25](https://github.com/bzkf/diz-in-a-box/issues/25)) ([8bd5be7](https://github.com/bzkf/diz-in-a-box/commit/8bd5be70cd5fd55b8e878db19786d3df180cf59b))
* **deps:** update harbor.miracum.org/streams-ume/onkoadt-to-fhir docker tag to v1.7.0 ([#32](https://github.com/bzkf/diz-in-a-box/issues/32)) ([0722f52](https://github.com/bzkf/diz-in-a-box/commit/0722f52062779f484ffee8407c35e0a08db50240))
* **deps:** update helm release akhq to v0.24.0 ([#21](https://github.com/bzkf/diz-in-a-box/issues/21)) ([bf25c89](https://github.com/bzkf/diz-in-a-box/commit/bf25c893307619dc191079b4334f2d8854c0edff))
* **deps:** update helm release fhir-gateway to v6.0.5 ([#23](https://github.com/bzkf/diz-in-a-box/issues/23)) ([6885339](https://github.com/bzkf/diz-in-a-box/commit/688533936e3763c58f599c2ff90229f0350f70b9))
* **deps:** update helm release hapi-fhir-jpaserver to v0.11.1 ([#5](https://github.com/bzkf/diz-in-a-box/issues/5)) ([3c18bcc](https://github.com/bzkf/diz-in-a-box/commit/3c18bcc801597e418cd6f697c7dff454b93b0ff2))
* **deps:** update ossf/scorecard-action action to v2.1.3 ([#20](https://github.com/bzkf/diz-in-a-box/issues/20)) ([a8bac91](https://github.com/bzkf/diz-in-a-box/commit/a8bac913502cdb4c2e8c50845324ff80dc7255c1))
* **deps:** update oxsecurity/megalinter action to v6.20.1 ([#6](https://github.com/bzkf/diz-in-a-box/issues/6)) ([c3afbf4](https://github.com/bzkf/diz-in-a-box/commit/c3afbf4b2d7813808c50a855a6387074080db2cd))
* **deps:** update sigstore/cosign-installer action to v3.0.2 ([#24](https://github.com/bzkf/diz-in-a-box/issues/24)) ([d7f882b](https://github.com/bzkf/diz-in-a-box/commit/d7f882b7741c42c81cbf92069b0cd54b2734ec2c))
* moved prerequisites in dedicated prerequsites chart ([6bb2feb](https://github.com/bzkf/diz-in-a-box/commit/6bb2feb2979e11735e5ec6149d9a6f49d47a073e))
* **renovate:** group dependency updates ([6bb2feb](https://github.com/bzkf/diz-in-a-box/commit/6bb2feb2979e11735e5ec6149d9a6f49d47a073e))

## 1.0.0 (2023-03-09)


### Features

* first version of the diz-in-a-box package and test workflow ([#1](https://github.com/bzkf/diz-in-a-box/issues/1)) ([fd6d6bc](https://github.com/bzkf/diz-in-a-box/commit/fd6d6bcf8b66025b2c4d6c121ada02c174517204))


### Miscellaneous Chores

* added LICENSE file ([623a449](https://github.com/bzkf/diz-in-a-box/commit/623a449dfc39477c97951428f2ae4ede818a7890))
* added version.txt to trigger release please ([f68b7d8](https://github.com/bzkf/diz-in-a-box/commit/f68b7d8852398e8032546458723d13b3de3170a0))


### CI/CD

* downgrade cosign version ([ba3eba2](https://github.com/bzkf/diz-in-a-box/commit/ba3eba29935b8ad42bab03c8bcdcdab21ad21fca))
* set release please token ([dcd913e](https://github.com/bzkf/diz-in-a-box/commit/dcd913e33acc6e40956368fafdcac2c961838158))
