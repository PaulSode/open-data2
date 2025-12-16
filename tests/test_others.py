import pytest
import pandas as pd
import pipeline.quality as quality
from pipeline.quality import QualityAnalyzer
from pipeline.storage import save_parquet, load_parquet, save_raw_json
from pipeline.enricher import DataEnricher
import pipeline.storage as storage
import json



class TestOthers:
    """Tests pour les autres fichiers."""

    #enricher.py
    def test_enrich_products_no_address(self):
        enricher = DataEnricher()
        products = [{"code": "1", "stores": None}]

        enriched = enricher.enrich_products(products, geocoding_cache={})

        assert enriched[0]["code"] == "1"


    #quality.py
    def test_quality_analyzer_basic(self):
        df = pd.DataFrame({
            "code": ["1", "2", "3"],
            "value": [10, 20, None],
            "geocoding_score": [0.8, None, 0.6]
        })

        analyzer = QualityAnalyzer(df)
        metrics = analyzer.analyze()

        assert metrics.total_records == 3
        assert metrics.quality_grade in ["A", "B", "C", "D", "F"]





    #storage.py
    def test_save_and_load_parquet(self):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

        path = save_parquet(df, "test_storage_parquet")
        df_loaded = load_parquet(path)

        assert df_loaded.equals(df)



    #main.py
    def test_run_pipeline_skip_enrichment(self, monkeypatch):
        import pandas as pd
        from pipeline.main import run_pipeline

        class FakeFetcher:
            def fetch_all(self, *args, **kwargs):
                return [{"code": "1", "stores": None}]
            def get_stats(self):
                return {}

        monkeypatch.setattr(
            "pipeline.main.OpenFoodFactsFetcher",
            lambda: FakeFetcher()
        )

        monkeypatch.setattr("pipeline.main.save_raw_json", lambda *a, **k: None)
        monkeypatch.setattr("pipeline.main.save_parquet", lambda df, name: "fake.parquet")
        monkeypatch.setattr(QualityAnalyzer, "generate_ai_recommendations", lambda self: "OK")


        stats = run_pipeline(
            category="test",
            max_items=1,
            skip_enrichment=True,
            verbose=False
        )

        assert "duration_seconds" in stats
