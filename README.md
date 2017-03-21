# openafs-stuff
Openafs related tools and projects.

OpenAFS Tools
-------------

Various tooling for working with the OpenAFS filesystem.

- [repserver.c] - Automated replication daemon for OpenAFS written in C
  - It automates the 'vos release' process thereby implementing lazy replication.
  - Uses a queue based mechanism and can release multiple volumes in parallel.
  - Will only attempt to release volumes that are out of sync with their RO counterparts.
