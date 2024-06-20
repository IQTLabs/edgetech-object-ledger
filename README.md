<a name="readme-top"></a>

[contributors-shield]: https://img.shields.io/github/contributors/IQTLabs/edgetech-template.svg?style=for-the-badge
[contributors-url]: https://github.com/IQTLabs/edgetech-template/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/IQTLabs/edgetech-template.svg?style=for-the-badge
[forks-url]: https://github.com/IQTLabs/edgetech-template/network/members
[stars-shield]: https://img.shields.io/github/stars/IQTLabs/edgetech-template.svg?style=for-the-badge
[stars-url]: https://github.com/IQTLabs/edgetech-template/stargazers
[issues-shield]: https://img.shields.io/github/issues/IQTLabs/edgetech-template.svg?style=for-the-badge
[issues-url]: https://github.com/IQTLabs/edgetech-template/issues
[license-shield]: https://img.shields.io/github/license/IQTLabs/edgetech-template.svg?style=for-the-badge
[license-url]: https://github.com/IQTLabs/edgetech-template/blob/master/LICENSE.txt
[product-screenshot]: images/screenshot.png
[python]: https://img.shields.io/badge/python-000000?style=for-the-badge&logo=python
[python-url]: https://www.python.org
[poetry]: https://img.shields.io/badge/poetry-20232A?style=for-the-badge&logo=poetry
[poetry-url]: https://python-poetry.org
[docker]: https://img.shields.io/badge/docker-35495E?style=for-the-badge&logo=docker
[docker-url]: https://www.docker.com

[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]

<br />
<div align="center">
  <a href="https://iqt.org">
    <img src="images/logo.png" alt="Logo" width="358" height="132">
  </a>
</div>
<h1 align="center">EdgeTech Object Ledger</h1>


This project manages a ledger of moving objects, such as aircraft or
ships, that can be used for pointing a camera at the object.

In more detail, the object ledger subscribes to MQTT message topics
for ADS-B messages, which provide aircraft position and velocity,
and AIS messages, which provide ship position and velocity. Each
messages corresponds to a uniquely identifiable object which is used
to update a ledger by unique identifier. A figure of merit is
computed for each object, and one object selected for tracking. The
ledger is maintained in a Pandas DataFrame, which is serialized to
local storage so that the history of objects selected for tracking
can be used by the figure of merit calculation. All object ledger
parameters can be customized through environment variables, or using
an MQTT message published to a configuration topic. Units of measure
are meters, seconds, and degrees, and operation of the object ledger
is extensively logged.

## Usage

This module is designed to be used in concert with other modules to
build a complete tracking system. [SkyScan](https://github.com/IQTLabs/SkyScan), 
which tracks aircraft using ADS-B transmissions, is an example of the type of 
system that can be built.

 Checkout the `docker-compose.yml` in that repo to see how these modules
 can be connected together. The configuration for the system is stored in `.env` environment files. Examples of the different environment files
 are included in the **SkyScan** repo and can be configured them to match your setup.

### Built With

[![Python][python]][python-url]
[![Poetry][poetry]][poetry-url]
[![Docker][docker]][docker-url]

## Roadmap

- TBA

See the [open
issues](https://github.com/IQTLabs/edgetech-object-ledger/issues) for a
full list of proposed features (and known issues).

## Contributing

1. Fork the Project
2. Create your Feature Branch (`git checkout -b dev`)
3. Commit your Changes (`git commit -m 'adding some feature'`)
4. Run (and make sure they pass):

```
black --diff --check *.py

pylint --disable=all --enable=unused-import *.py

mypy --allow-untyped-decorators --ignore-missing-imports --no-warn-return-any --strict --allow-subclassing-any *.py
```

If you do not have them installed, you can install them with `pip
install "black<23" pylint==v3.0.0a3 mypy==v0.991`.

5. Push to the Branch (`git push origin dev`)
6. Open a Pull Request

See `CONTRIBUTING.md` for more information.

## License

Distributed under the [Apache
2.0](https://github.com/IQTLabs/edgetech-object-ledger/blob/main/LICENSE). See
`LICENSE.txt` for more information.

## Contact IQTLabs

- Twitter: [@iqtlabs](https://twitter.com/iqtlabs)
- Email: labsinfo@iqt.org

See our other projects: [https://github.com/IQTLabs/](https://github.com/IQTLabs/)

<p align="right">(<a href="#readme-top">back to top</a>)</p>
