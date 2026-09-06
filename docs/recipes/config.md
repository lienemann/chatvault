# Configuring chatvault

chatvault reads `config.toml` from its config dir on startup. The location
follows XDG; on Termux that's `~/.config/chatvault/config.toml`.

This file is for non-secret preferences and **paths to secret files** —
never for secret values themselves.

```sh
# Show where config.toml is expected (banner prints the package path, not the
# config path — derive it like this):
echo ${XDG_CONFIG_HOME:-$HOME/.config}/chatvault/config.toml
```

## Schema

Every commented line below shows the **default**. Uncomment + change to
override. Unknown keys are ignored, so old configs don't break on upgrade.

```toml
[owner]
# name = "Me"

[anthropic]
# api_key_env = "ANTHROPIC_API_KEY"
# api_key_file = ""        # absolute path; reader requires chmod 600

[openai]
# api_key_env = "OPENAI_API_KEY"
# api_key_file = ""

[signal]
# passphrase_file = ""     # chmod-600 file with the 30-digit backup passphrase
```

## API keys

There are two ways to provide an API key. Pick **one** per provider:

1. **Environment variable** (default). chatvault reads `$ANTHROPIC_API_KEY`
   for Claude and `$OPENAI_API_KEY` for OpenAI-compatible endpoints. The
   variable name is overridable via `api_key_env`.

2. **chmod-600 file**. Put the key in a file owned by you with mode `600`
   and point `api_key_file` at it. chatvault hard-fails if permissions are
   looser, so a leaked key from a shared shell becomes a clear error rather
   than a silent cost.

```sh
mkdir -p ~/.config/chatvault
umask 077
printf '%s' 'sk-ant-…' > ~/.config/chatvault/anthropic.key
chmod 600 ~/.config/chatvault/anthropic.key
```

```toml
[anthropic]
api_key_file = "~/.config/chatvault/anthropic.key"
```

### Resolution order

For every call that needs an API key:

1. The env var named by `api_key_env` (or its default), if set.
2. `api_key_file`, if configured.
3. Hard error with both options listed.

Env wins over file so a one-off `ANTHROPIC_API_KEY=… chatvault …` always
works regardless of what config.toml says.

## Signal passphrase

Set `[signal].passphrase_file` to a chmod-600 file holding the 30-digit
passphrase. Spaces are optional — Signal's KDF strips them, so either of
these works:

```
00000 11111 22222 33333 44444 55555
```

```
000001111122222333334444455555
```

Resolution order for `chatvault signal extract`:

1. `--passphrase` CLI flag.
2. `$SIGNAL_BACKUP_PASSPHRASE` (or the env var named by `--passphrase-env`).
3. `--passphrase-file PATH` CLI flag.
4. `[signal].passphrase_file` in config.toml.
5. Interactive prompt.

## What does NOT belong here

- The 64-char WhatsApp backup key → lives in `~/.config/chatvault/wa.key`
  (chmod 600, managed by `chatvault key set`).
- Any one-shot path you only use once — keep it on the CLI.

Mixing secret *values* (as opposed to paths) into a single config file makes
it dangerously easy to commit them to git. The chmod-600 file pattern keeps
each secret in its own restricted blob.
