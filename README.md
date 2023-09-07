# Custom Codecs
Custom Codecs plugin makes it possible for users to provide custom Lucene codecs for loading through Apache Lucene's `NamedSPILoader`.
These codecs can be used to customize the on-disk representation of the opensearch indexes. For example, zstd compression can be used for 
`StoredField` types through the `ZstdCodec`.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

