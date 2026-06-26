"""
Document Factory - Unified document processing factory
Integrates ArXiv, PDF, and README processors using the DocumentProcessor base
"""

import logging
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio

from core.bounded_workers import map_bounded, resolve_worker_concurrency
from core.data_models import Tweet, ProcessingStats
from core.config import config
from .arxiv_processor_v2 import ArXivProcessorV2
from .pdf_processor import PDFProcessor
from .readme_processor import READMEProcessor

logger = logging.getLogger(__name__)


class DocumentFactory:
    """Factory for unified document processing"""
    
    def __init__(self, vault_path: str = None):
        self.arxiv_processor = ArXivProcessorV2(vault_path)
        self.pdf_processor = PDFProcessor(vault_path)
        self.readme_processor = READMEProcessor(vault_path)
        self.concurrent_workers = resolve_worker_concurrency(
            config.get('processing.documents.concurrent_workers', 3),
            default=3,
            setting_name='processing.documents.concurrent_workers',
        )

        self.processors = {
            'arxiv': self.arxiv_processor,
            'pdf': self.pdf_processor,
            'readme': self.readme_processor
        }
    
    def process_all_documents(self, tweets: List[Tweet], resume: bool = True, 
                             concurrent: bool = True) -> Dict[str, ProcessingStats]:
        """Process all document types for tweets"""
        if concurrent:
            return self._process_concurrent(tweets, resume)
        else:
            return self._process_sequential(tweets, resume)
    
    def _process_sequential(self, tweets: List[Tweet], resume: bool) -> Dict[str, ProcessingStats]:
        """Process documents sequentially"""
        stats = {}
        
        logger.info("📄 Processing ArXiv papers...")
        stats['arxiv'] = self.arxiv_processor.process_tweets(tweets, resume)
        
        logger.info("📄 Processing PDF documents...")
        stats['pdf'] = self.pdf_processor.process_tweets(tweets, resume)
        
        logger.info("📂 Processing repository READMEs...")
        stats['readme'] = self.readme_processor.process_tweets(tweets, resume)
        
        return stats
    
    def _process_concurrent(self, tweets: List[Tweet], resume: bool) -> Dict[str, ProcessingStats]:
        """Process documents concurrently using ThreadPoolExecutor"""
        stats = {}
        
        logger.info("📄 Processing documents concurrently...")
        
        with ThreadPoolExecutor(max_workers=self.concurrent_workers) as executor:
            # Submit all processors
            futures = {
                executor.submit(self.arxiv_processor.process_tweets, tweets, resume): 'arxiv',
                executor.submit(self.pdf_processor.process_tweets, tweets, resume): 'pdf',
                executor.submit(self.readme_processor.process_tweets, tweets, resume): 'readme'
            }
            
            # Collect results
            for future in as_completed(futures):
                processor_type = futures[future]
                try:
                    result = future.result()
                    stats[processor_type] = result
                    logger.info(f"✅ {processor_type.upper()} processing complete: "
                              f"{result.updated} processed, {result.skipped} skipped, "
                              f"{result.errors} errors")
                except Exception as e:
                    logger.error(f"❌ {processor_type.upper()} processing failed: {e}")
                    # Create empty stats for failed processor
                    stats[processor_type] = ProcessingStats()
                    stats[processor_type].errors = 1
        
        return stats
    
    async def process_single_tweet_async(self, tweet: Tweet, resume: bool = True) -> Dict[str, Any]:
        """Process all document types for a single tweet asynchronously"""
        results = {
            'arxiv': [],
            'pdf': [],
            'readme': []
        }
        
        work_items = []
        
        # ArXiv processing
        arxiv_urls = self.arxiv_processor.extract_urls_from_tweet(tweet)
        for url in arxiv_urls:
            work_items.append(('arxiv', self.arxiv_processor, url))
        
        # PDF processing
        pdf_urls = self.pdf_processor.extract_urls_from_tweet(tweet)
        for url in pdf_urls:
            work_items.append(('pdf', self.pdf_processor, url))
        
        # README processing
        readme_urls = self.readme_processor.extract_urls_from_tweet(tweet)
        for url in readme_urls:
            work_items.append(('readme', self.readme_processor, url))

        async def download_item(item):
            doc_type, processor, url = item
            try:
                document = await self._download_document_async(
                    processor,
                    url,
                    tweet.id,
                    resume,
                )
                return doc_type, document
            except Exception as e:
                logger.error(f"Failed to process {doc_type} document for tweet {tweet.id}: {e}")
                return doc_type, None

        for doc_type, document in await map_bounded(
            work_items,
            download_item,
            concurrency=self.concurrent_workers,
        ):
            if document:
                results[doc_type].append(document)
        
        # Attach documents to tweet
        if results['arxiv']:
            tweet.arxiv_papers = results['arxiv']
        if results['pdf']:
            tweet.pdf_links = results['pdf']
        if results['readme']:
            tweet.repo_links = results['readme']
        
        return results
    
    async def _download_document_async(self, processor, url: str, tweet_id: str, resume: bool):
        """Download a document asynchronously using asyncio"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, processor.download_document, url, tweet_id, resume)
    
    def get_summary_stats(self, all_stats: Dict[str, ProcessingStats]) -> str:
        """Generate a summary of all document processing stats"""
        summary = []
        
        for doc_type, stats in all_stats.items():
            if stats.total_processed > 0:
                summary.append(f"{doc_type.upper()}: {stats.updated} processed, "
                             f"{stats.skipped} skipped, {stats.errors} errors")
        
        return "; ".join(summary) if summary else "No documents processed"
    
    def get_processor(self, document_type: str):
        """Get a specific processor by type"""
        return self.processors.get(document_type.lower())
    
    def supports_document_type(self, document_type: str) -> bool:
        """Check if a document type is supported"""
        return document_type.lower() in self.processors
