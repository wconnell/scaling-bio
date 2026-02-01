# Scaling Biology

Tracking the growth of biological data. [Contributions welcome!](#contributing)

This project is a follow up to my Substack posts on the subject:
- [Scaling biology: transcriptomics](https://behindbioml.substack.com/p/scaling-biology-part-1), 4/2024
- [Scaling biology: genomics](https://behindbioml.substack.com/p/scaling-biology-genomics), 8/2024

**🌐 Live site:** [wconnell.github.io/scaling-bio](https://wconnell.github.io/scaling-bio)



## Methodology

Data is collected weekly via automated GitHub Actions from primary sources. Each data source uses a dedicated collector that fetches current statistics and historical timeseries.

### SRA (Sequence Read Archive)

- **Metric:** Total sequenced bases
- **Source:** [NIH SRA BigQuery Public Dataset](https://www.ncbi.nlm.nih.gov/sra/docs/sra-bigquery/)
- **Query:** Aggregates `mbases` column by release year from `nih-sra-datastore.sra.metadata`
- **Update frequency:** Weekly

### CellxGene Census

- **Metric:** Unique single cells profiled
- **Source:** [CZI CellxGene Census API](https://chanzuckerberg.github.io/cellxgene-census/)
- **Method:** Queries `census_info.summary` for official `unique_cell_count`. Timeseries derived from dataset publication dates via CrossRef API, scaled to match official totals.
- **Update frequency:** Quarterly (follows Census releases)

### PDB (Protein Data Bank)

- **Metric:** Experimentally determined structures
- **Source:** [RCSB PDB Search API](https://search.rcsb.org/)
- **Method:** Queries total structure count and groups by release year
- **Update frequency:** Weekly

### GenBank

- **Metric:** Total nucleotide bases
- **Source:** [NCBI GenBank FTP Release Notes](https://ftp.ncbi.nih.gov/genbank/release.notes/)
- **Method:** Parses `gb*.release.notes` files for base counts from each release
- **Update frequency:** Bi-monthly

### UniProt

- **Metric:** Protein sequences in UniProtKB
- **Source:** [UniProt FTP Release Archives](https://ftp.uniprot.org/pub/databases/uniprot/previous_releases/)
- **Method:** Parses `relnotes.txt` from each yearly release to extract entry counts
- **Update frequency:** Monthly

## Contributing

```bash
git clone https://github.com/wconnell/scaling-bio.git
cd scaling-bio
uv venv && source .venv/bin/activate
uv pip install -r requirements.txt
```

To add a new data source:

1. **Copy an existing collector** (e.g., `collectors/pdb_collector.py`) and modify:
   - `source_id` / `source_info` - metadata for your source
   - `collect()` - fetch raw data from API/URL, save to `self.data_dir`
   - `transform()` - convert to `CollectorOutput` with metrics + timeseries

2. **Register** in `collectors/registry.py`:
   ```python
   from .my_collector import MyCollector
   COLLECTORS = [..., MyCollector]
   ```

3. **Test:**
   ```bash
   python scripts/collect_all.py
   cd site && python -m http.server 8888
   ```

4. **PR** with collector + methodology entry in this README

**Requirements:**
- Primary sources only (official APIs/stats, not secondary aggregators)
- Historical timeseries, not just current totals
- Metric = "size" of data (bases, cells, structures, sequences, etc.)

## License

Data sourced from public repositories under their respective licenses. Code is MIT licensed.
