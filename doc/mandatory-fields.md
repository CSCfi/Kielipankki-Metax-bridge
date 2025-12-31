# Mandatory Fields in COMEDI for Metax Export

## General Guidelines

### Language Variants

Many fields can have different language variants in COMEDI. The languages
included in Metax export are "en", "fi" and "und": if multiple are provided,
all are included in the Metax record.

### Metax Documentation

For more information on Metax and it's data model, see:
- [Metax user guide](https://metax.fairdata.fi/v3/docs/user-guide/) (much of
  the relevant information regarding constraints can be found under
  [Differences between V1-V2 and V3](https://metax.fairdata.fi/v3/docs/user-guide/migration/))
- [Swagger](https://metax.fairdata.fi/v3/swagger/) (technical description of
  the API: hard to read, but more comprehensive than the user docs)

## Data Fields

### PID

The identifier for the record is parsed from the *self link* field, and is
expected to start with "urn:nbn:fi:lb-" proceeded by an arbitrary string. The
self link field should be filled as soon as the record is created, as records
without it will not e.g. be included in the backups.

### Title

Title is parsed from the *resource name* field under *identification info*. If
multiple language variants are given, they are presented according to the [language
rules](#language-variants). Each record must have a title in at least one
supported language.

### Description

Description is parsed from the *description* field under *identification info*.
multiple language variants are given, they are presented according to the [language
rules](#language-variants).

### Language

Languages are parsed from *language id* fields in *language info* under *corpus
info*. If the same language id appears multiple times (e.g. "fi" for both
"Standard Finnish" and "Easy-to-read Finnish" as language name), the duplicates
are discarded. The language name field is ignored.

The language IDs can be provided either as short 2-letter language codes (e.g.
"fi") or the long 3-letter ones (e.g. "fin"), but Metax only accepts Lexvo URIs
in form http://lexvo.org/id/iso639-3/LLL or similar for ISO 639-5, where LLL is
the three-letter language code from ISO 639-3. This requires translating the
codes from one standard to another and prepending the lexvo URI.  When
available, ISO 639-3 is preferred.


### Access Rights

Access rights are determined based on *licence* names and *availability* under
*licence info*.

License names are mapped to the [reference data
values](https://koodistot.suomi.fi/codescheme;registryCode=fairdata;schemeCode=license)
using the following conversion scheme:

| COMEDI            | Metax                                                               |
| ----------------- | --------------------------------------------------------------------|
| CLARIN_PUB        | http://uri.suomi.fi/codelist/fairdata/license/code/ClarinPUB-1.0    |
| CLARIN_ACA        | http://uri.suomi.fi/codelist/fairdata/license/code/ClarinACA-1.0    |
| CLARIN_ACA-NC     | http://uri.suomi.fi/codelist/fairdata/license/code/ClarinACA+NC-1.0 |
| CLARIN_RES        | http://uri.suomi.fi/codelist/fairdata/license/code/ClarinRES-1.0    |
| other             | http://uri.suomi.fi/codelist/fairdata/license/code/other            |
| underNegotiation  | http://uri.suomi.fi/codelist/fairdata/license/code/undernegotiation |
| proprietary       | http://uri.suomi.fi/codelist/fairdata/license/code/other-closed     |
| CC-BY             | http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-1.0        |
| CC-BY-ND          | http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-ND-4.0     |
| CC-BY-NC          | http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-NC-2.0     |
| CC-BY-SA          | http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-SA-3.0     |
| CC-BY-NC-ND       | http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-NC-ND-4.0  |
| CC-BY-NC-SA       | http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-NC-SA-4.0  |
| CC-ZERO           | http://uri.suomi.fi/codelist/fairdata/license/code/CC0-1.0          |
| ApacheLicence_2.0 | http://uri.suomi.fi/codelist/fairdata/license/code/Apache-2.0       |

If unmapped licence names are encountered, they are skipped. If that would
result in a record with no licences, an "other" licence is set.

The access type is determined based on a combination of *availability*: for
corpora that are "available-unrestrictedUse", the access type is
[open](http://uri.suomi.fi/codelist/fairdata/access_type/code/open), all others
are
[restricted](http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted).
For restricted corpora, this is accompanied with restriction grounds: for ACA
resources, this is
[research](http://uri.suomi.fi/codelist/fairdata/restriction_grounds/code/research),
but other restriction grounds cannot be reliably mapped from the
machine-readable data, so all other corpora get
[other](http://uri.suomi.fi/codelist/fairdata/restriction_grounds/code/other).

### Actors

Actors can be either persons or organizations. Each person must be affiliated
to an organization.

Organizations are represented by [reference
data](https://koodistot.suomi.fi/codescheme;registryCode=fairdata;schemeCode=organization)
links, using the following mapping:

| Organization | Code in reference data |
| ------------ | ---------------------- |
| Aalto University | 10076 |
| CSC — IT Center for Science Ltd | 09206320 |
| Centre for Applied Language Studies | 01906-213060 |
| FIN-CLARIN | 01901 |
| National Library of Finland | 01901-H981 |
| South Eastern Finland University of Applied Sciences | 10118 |
| University of Eastern Finland | 10088 |
| University of Helsinki | 01901 |
| University of Jyväskylä | 01906 |
| University of Oulu | 01904 |
| University of Tampere | 10122 |
| University of Turku | 10089 |

For more information on actors in Metax, see
[https://metax.fairdata.fi/v3/docs/user-guide/datasets-api/#actors](https://metax.fairdata.fi/v3/docs/user-guide/datasets-api/#actors)


#### Creator

Each dataset must have at least one creator. This information is parsed from
*resource creator person* or *resource creator organization* under *resource
creation info*.

#### Publisher
Each dataset in Metax must have exactly one publisher. The corresponding field
in COMEDI is *distribution rights holder person* / *distribution rights holder
organization*.

If there are multiple distribution rightsholders, Metax is supplied with
"Multiple publishers, check distribution rights holders in original metadata by
following its persistent identifier" instead of the actual publisher
information (see e.g. [lb-2016051602 in
Etsin](https://etsin.fairdata.fi/dataset/8b9b11fb-52df-4654-9443-ca03b8a83fa1).

#### Curator

Curator is an optional field and is determined by *contact person*.

#### Rights holder

Rights holders are determined by *IPR holder person* and *IPR holder
organization* in COMEDI.
