Frozen copy of the v1 importer (`revolut_to_ynab.py` at commit b1dc53a).

It is used only by tests, as an oracle:

* golden tests check that v2's Revolut parser derives byte-identical import_ids,
  amounts, dates and memos;
* migration tests drive v1's import flow against the fake YNAB to build realistic
  legacy databases, then migrate them to the v2 schema.

Do not edit or import it from production code.
