"""Module d'enrichissement des données."""
from tqdm import tqdm

from .fetchers.adresse import AdresseFetcher
from .models import GeocodingResult


class DataEnricher:
    """Enrichit les données en croisant plusieurs sources."""
    
    def __init__(self):
        self.geocoder = AdresseFetcher()
        self.enrichment_stats = {
            "total_processed": 0,
            "successfully_enriched": 0,
            "failed_enrichment": 0,
        }

    def extract_addresses(self, products: list[dict], address_field: str = "stores") -> list[str]:
        """
        Extrait les adresses uniques normalisées depuis les produits.
        """
        addresses = set()

        for product in products:
            raw_addr = product.get(address_field)

            if not raw_addr or not isinstance(raw_addr, str):
                continue

            normalized = self.normalize_address(raw_addr)

            # Filtrage minimal pour éviter le bruit
            if len(normalized) < 4 or len(normalized.split()) < 2:
                continue

            addresses.add(normalized)

        return list(addresses)

    
    def build_geocoding_cache(self, addresses: list[str]) -> dict[str, GeocodingResult]:
        """
        Construit un cache de géocodage pour éviter les requêtes en double.
        
        Args:
            addresses: Liste d'adresses à géocoder
        
        Returns:
            Dictionnaire adresse -> résultat
        """
        cache = {}
        
        print(f"🌍 Géocodage de {len(addresses)} adresses uniques...")
        
        for result in self.geocoder.fetch_all(addresses):
            cache[result.original_address] = result
        
        success_rate = sum(1 for r in cache.values() if r.is_valid) / len(cache) * 100 if cache else 0
        print(f"✅ Taux de succès: {success_rate:.1f}%")
        
        return cache
    
    def enrich_products(self, products: list[dict], geocoding_cache: dict[str, GeocodingResult], address_field: str = "stores") -> list[dict]:
        """
        Enrichit les produits avec les résultats de géocodage.
        """
        enriched_products = []

        for product in tqdm(products, desc="Enrichissement"):
            self.enrichment_stats["total_processed"] += 1

            enriched = product.copy()
            raw_addr = product.get(address_field)

            if not raw_addr or not isinstance(raw_addr, str):
                self.enrichment_stats["failed_enrichment"] += 1
                enriched_products.append(enriched)
                continue

            normalized = self.normalize_address(raw_addr)

            geo = geocoding_cache.get(normalized)

            if not geo or not geo.is_valid:
                self.enrichment_stats["failed_enrichment"] += 1
                enriched_products.append(enriched)
                continue

            enriched["store_address"] = geo.label
            enriched["latitude"] = geo.latitude
            enriched["longitude"] = geo.longitude
            enriched["city"] = geo.city
            enriched["postal_code"] = geo.postal_code
            enriched["geocoding_score"] = geo.score

            self.enrichment_stats["successfully_enriched"] += 1
            enriched_products.append(enriched)

        return enriched_products

    
    def get_stats(self) -> dict:
        """Retourne les statistiques d'enrichissement."""
        stats = self.enrichment_stats.copy()
        stats["geocoder_stats"] = self.geocoder.get_stats()
        
        if stats["total_processed"] > 0:
            stats["success_rate"] = stats["successfully_enriched"] / stats["total_processed"] * 100
        else:
            stats["success_rate"] = 0
        
        return stats
    
    def normalize_address(self, addr: str) -> str:
        """
        Normalise une adresse pour garantir la cohérenceentre extraction cache et enrichissement.
        """
        if not addr or not isinstance(addr, str):
            return ""

        return addr.split(",")[0].strip().lower()