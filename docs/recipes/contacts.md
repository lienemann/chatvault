# Contacts

Without contacts, every sender shows up as `+phone`. `chatvault contact list`
shows what's in the table; `unresolved` shows who's still missing. Four
sources exist; the ranking when they overlap is **manual > vcard > address_book**.

## See what's in the archive

```sh
chatvault contact list                       # all contacts
chatvault contact list --source manual       # filter by source
chatvault contact unresolved                 # active senders WITHOUT a contact
chatvault contact unresolved --csv pins.csv  # dump as a CSV starter file
```

## Source 1 — Address-book sync (single primary number per contact)

`termux-contact-list` returns one phone number per contact. For multi-number
people, that loses linkage. Good for daily cron.

```sh
chatvault contact sync
```

Reconciles add/remove against `source='address_book'` rows only. **Manual
pins are never touched.**

## Source 2 — vCard export (every number for every contact)

Export the whole address book as a vCard file from the Contacts app, then:

```sh
chatvault contact import-vcard ~/storage/downloads/contacts.vcf
```

On Android (Google Contacts): **Contacts → ☰ → Settings → Export → All
contacts**. File typically lands in `Internal storage/Download/`.

The import is **additive**: rows from `contact sync` aren't deleted, and
manual pins are preserved. Re-run anytime your address book grows.

## Source 3 — Manual pins (overrides everything)

For people who aren't in your address book at all (group strangers, push-name
contacts), or whose name you want to override locally:

```sh
chatvault contact pin "+49 172 3105522" "Raphael"
chatvault contact pin 491723105522@s.whatsapp.net "Raphael"
chatvault contact unpin "+49 172 3105522"
```

Pins live both in the DB (`source='manual'`) and in a JSON sidecar at
`~/.config/chatvault/manual_contacts.json` (chmod 0600). The sidecar
survives a full archive re-init — `extract` automatically re-applies pins.

## Source 4 — Bulk pins from CSV

For batches:

```sh
# Get a starter file pre-filled with active senders that have no name:
chatvault contact unresolved --min-messages 50 --csv pins.csv
# Edit pins.csv (fill names), then:
chatvault contact import-csv pins.csv
```

CSV format: `phone,name` header required. Variants `number,display_name`
are also accepted. Each row → manual pin.

## Inspecting / debugging

```sh
sqlite3 ~/.local/share/chatvault/archive.db \
  "SELECT phone_jid, name, source, updated_at FROM contacts \
   WHERE phone_jid = '41764475119@s.whatsapp.net'"
```

History (every observation) is in `contacts_history`. To trace why a
specific JID resolves to a specific name:

```sh
chatvault chat why "Girls Night" "196323038986257@lid"
```
