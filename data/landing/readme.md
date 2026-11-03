# Landing Zone

Place your Cashew database exports in this folder. The ingestion pipeline picks up the latest file that has not been loaded yet.

## Steps

1. **Upload** your Cashew data file here.
2. **Rename the extension.** Cashew exports end in `.sql`. Change it to `.sqlite`.
3. **Keep the original file name pattern.** Only the extension should change.

## File name pattern

Files must match this format, or they are ignored:

```
cashew-db-v<version>-<anything>-<YYYY>-<MM>-<DD>-<HH>-<mm>-<ss>-<SSS>Z.sqlite
```

Example:

```
cashew-db-v19-2026-10-01-08-30-15-123Z.sqlite
```

The timestamp in the name decides which file is the latest.

## Notes

- Only the latest file is loaded, and only if it has not been loaded before.
- Files that don't end in `.sqlite` or don't match the pattern are skipped silently.