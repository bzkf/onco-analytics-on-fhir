# Changelog

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
