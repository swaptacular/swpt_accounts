+++++++++++++++++++++++++++++++++
``RootConfigData`` JSON Documents
+++++++++++++++++++++++++++++++++
:Description: Specifies the format for RootConfigData documents.
:Author: Evgeni Pandurksi
:Contact: epandurski@gmail.com
:Date: 2023-08-23
:Version: 1.0
:Copyright: This document has been placed in the public domain.


Overview
========

This document specifies the format for ``RootConfigData`` documents.

In `Swaptacular`_, each *currency issuer* is able to configure various
important parameters of its issued currency (the annual interest rate,
for example). In order to take effect, the chosen currency parameters
must be put together in a machine-readable document, and sent over the
network, to the *accounting authority node* that is responsible for
managing the currency. The ``RootConfigData`` document format,
specified here, is one of the standard machine-readable formats that
can be used to transmit the chosen currency parameters. Note that in
Swaptacular's terminology, the word "debtor" means a Swaptacular
currency, with its respective issuer.

**Note:** The key words "MUST", "MUST NOT", "REQUIRED", "SHALL",
"SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and
"OPTIONAL" in this document are to be interpreted as described in
RFC 2119.


The Debtor's Account
====================

The `Swaptacular Messaging Protocol`_ (SMP) governs the network
communication between accounting authority nodes and their peer
*debtors agent nodes*. In SMP, every account is uniquely identified by
a ``(debtor_id``, ``creditor_id)`` number pair.

To to issue new tokens into existence, and to manage the various
important currency parameters, a special account called "*the debtor's
account*" (or "the root account") is used. The ``creditor_id`` for the
debtor's account is ``0``.

To transmit their chosen currency parameters, debtors use the
``config_data`` text field of their root accounts. Accounting
authority nodes MAY support different formats for the ``config_data``
text field, but all accounting authority nodes MUST understand and
support the ``RootConfigData`` format specified here.


``RootConfigData`` Documents Structure
======================================

``RootConfigData`` documents are `JSON`_ documents whose structure and
content can be correctly validated by the `JSON Schema`_ specified
below.

`UTF-8`_ encoding MUST always be used for ``RootConfigData``
documents.


MIME Type
=========

Over HTTP connections, ``RootConfigData`` documents SHOULD be
transferred with ``application/vnd.swaptacular.root-config-data+json``
`MIME type`_.


JSON Schema
===========

Type: ``object``

path: #

This schema accepts additional properties.

Properties
==========

- **type** ``required``

  - Type: ``string``
  - path: #/properties/type
  - The value must match this pattern: ``^RootConfigData(-v[1-9][0-9]{0,5})?$``

- **rate**
   
  Optional annual rate (in percents) at which interest accumulates on
  creditors' accounts. Accounting authority nodes MAY decide to limit
  the range of allowed values for this parameter. However, zero MUST
  always be allowed, and all values between -50 an 100 SHOULD be
  allowed. Note that values smaller than -100 do not make sense, and
  SHOULD not be allowed.
   
  - Type: ``number``
  - path: #/properties/rate
  - Default: ``0.0``

- **limit**

  Optional limit for the total issued amount. The value must be a
  non-negative 64-bit integer. Note that the correct handling of
  integers outside the safe range from ``-(2 ** 53 - 1)`` to ``2 **
  53`` may be a problem for some JSON parsers and serializers.

  - Type: ``integer``
  - path: #/properties/limit
  - Range: between 0 and 9223372036854775807
  - Default: ``9223372036854775807``

- **info**

  Optional additional information about the debtor.

  - path: #/properties/info
  - &ref: `#/definitions/DebtorInfo`_


Definitions
===========


.. _`#/definitions/DebtorInfo`:
     
``DebtorInfo``
--------------

Type: ``object``

path: #/definitions/DebtorInfo

This schema accepts additional properties.

Properties
``````````
- **type** ``required``

  - Type: ``string``
  - path: #/definitions/DebtorInfo/properties/type
  - The value must match this pattern: ``^DebtorInfo(-v[1-9][0-9]{0,5})?$``

- **iri** ``required``

  A link (Internationalized Resource Identifier) referring to a
  document containing information about the debtor.

  - Type: ``string``
  - path: #/definitions/DebtorInfo/properties/iri
  - String format must be a "iri"
  - Length: between 1 and 200

- **contentType**

  Optional MIME type of the document that the ``iri`` field refers to.

  - Type: ``string``
  - path: #/definitions/DebtorInfo/properties/contentType
  - Length:  <= 100

- **sha256**

  Optional SHA-256 cryptographic hash (Base16 encoded) of the content
  of the document that the ``iri`` field refers to.

  - Type: ``string``
  - path: #/definitions/DebtorInfo/properties/sha256
  - The value must match this pattern: `^[0-9A-F]{64}$`


JSON Schema File
================

This is the JSON Schema file, for validating ``RootConfigData``
documents::
  
  {
    "definitions": {
      "DebtorInfo": {
        "type": "object",
        "properties": {
          "type": {
            "type": "string",
            "pattern": "^DebtorInfo(-v[1-9][0-9]{0,5})?$"
          },
          "iri": {
            "type": "string",
            "minLength": 1,
            "maxLength": 200,
            "format": "iri",
          },
          "contentType": {
            "type": "string",
            "maxLength": 100,
          },
          "sha256": {
            "type": "string",
            "pattern": "^[0-9A-F]{64}$",
          }
        },
        "required": [
          "type",
          "iri"
        ],
        "additionalProperties": true
      }
    },
    "type": "object",
    "properties": {
      "type":  {
        "type": "string",
        "pattern": "^RootConfigData(-v[1-9][0-9]{0,5})?$"
      },
      "rate": {
        "type": "number",
        "format": "float",
        "default": 0.0,
      },
      "limit": {
        "type": "integer",
        "format": "int64",      
        "minimum": 0,
        "maximum": 9223372036854775807,
        "default": 9223372036854775807,
      },
      "info": {
        "$ref": "#/definitions/DebtorInfo",
      }
    },
    "required": [
      "type"
    ],
    "additionalProperties": true
  }


.. _Swaptacular: https://swaptacular.github.io/overview
.. _Swaptacular Messaging Protocol: https://swaptacular.github.io/public/docs/protocol.pdf
.. _MIME Type: https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types
.. _UTF-8: https://en.wikipedia.org/wiki/UTF-8
.. _JSON: https://www.json.org/json-en.html
.. _JSON Schema: http://json-schema.org/
.. _URL: https://en.wikipedia.org/wiki/URL
.. _IRI: https://en.wikipedia.org/wiki/Internationalized_Resource_Identifier
