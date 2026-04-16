#!/usr/bin/env python3
"""
5BY Pack Boundary Worker

Render Standard Background Worker for:
- Post-Pack diagnostics (v1 enabled)
- Shadow baseline comparison (sampling only)
- Queue-based async processing

v1 Constraints:
- NO context_packs modification
- NO raw_messages modification
- NO authoritative Pack decision
- job_type 'pack_admission' is DB-blocked
"""

import os
import time
import logging
from datetime import datetime
from typing import Optional, Dict, Any

# Supabase client
from supabase import create_client, Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('5by-pack-worker')

# Worker version
WORKER_VERSION = 'v0.1.0'

# Polling interval (seconds)
POLL_INTERVAL = 10


def get_supabase_client() -> Optional[Client]:
    """Initialize Supabase client from environment variables."""
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_SERVICE_KEY')
    
    if not url or not key:
        logger.warning('SUPABASE_URL or SUPABASE_SERVICE_KEY not set')
        return None
    
    return create_client(url, key)


def claim_job(supabase: Client, job_id: str) -> bool:
    """
    Atomically claim a job by updating status from pending to processing.
    Returns True if claim succeeded, False if job was already claimed.
    """
    try:
        result = supabase.table('worker_jobs').update({
            'status': 'processing',
            'picked_at': datetime.utcnow().isoformat(),
            'worker_id': f'render-{os.getenv("RENDER_INSTANCE_ID", "local")}',
            'worker_version': WORKER_VERSION,
            'attempts': supabase.table('worker_jobs').select('attempts').eq('id', job_id).single().execute().data['attempts'] + 1
        }).eq('id', job_id).eq('status', 'pending').execute()
        
        # If no rows updated, job was already claimed
        return len(result.data) > 0
    except Exception as e:
        logger.error(f'Failed to claim job {job_id}: {e}')
        return False


def poll_jobs(supabase: Client) -> Optional[Dict[str, Any]]:
    """
    Poll for pending jobs ordered by priority and age.
    Returns the first pending job or None.
    """
    try:
        result = supabase.table('worker_jobs').select('*').eq(
            'status', 'pending'
        ).order(
            'priority', desc=True
        ).order(
            'created_at', desc=False
        ).limit(1).execute()
        
        if result.data:
            return result.data[0]
        return None
    except Exception as e:
        logger.error(f'Failed to poll jobs: {e}')
        return None


def process_job(supabase: Client, job: Dict[str, Any]) -> bool:
    """
    Process a job based on job_type.
    v1: Only post_pack_diagnostics is fully enabled.
    
    Returns True if successful, False otherwise.
    """
    job_id = job['id']
    job_type = job['job_type']
    pack_id = job.get('pack_id')
    payload = job.get('payload', {})
    
    logger.info(f'Processing job {job_id} type={job_type} pack_id={pack_id}')
    
    start_time = time.time()
    
    try:
        if job_type == 'post_pack_diagnostics':
            # v1: Record diagnostics only (no Pack modification)
            diagnostics_data = {
                'job_id': job_id,
                'pack_id': pack_id,
                'rule_engine_result': payload.get('cheap_signals', {}),
                'shadow_result': None,  # Future: Dialogue-Topic-Segmenter
                'reason_codes': [],
                'agreement': None,
                'latency_ms': int((time.time() - start_time) * 1000),
                'worker_version': WORKER_VERSION
            }
            
            supabase.table('worker_diagnostics').insert(diagnostics_data).execute()
            logger.info(f'Job {job_id}: diagnostics recorded')
            
        elif job_type == 'shadow_baseline':
            # v1: Sampling only - just log for now
            logger.info(f'Job {job_id}: shadow_baseline (sampling mode, no-op)')
            
        else:
            # Future gated job types
            logger.warning(f'Job {job_id}: job_type {job_type} not enabled in v1, skipping')
            mark_job_skipped(supabase, job_id, f'{job_type} not enabled in v1')
            return True
        
        # Mark job as completed
        latency_ms = int((time.time() - start_time) * 1000)
        supabase.table('worker_jobs').update({
            'status': 'completed',
            'completed_at': datetime.utcnow().isoformat()
        }).eq('id', job_id).execute()
        
        logger.info(f'Job {job_id} completed in {latency_ms}ms')
        return True
        
    except Exception as e:
        logger.error(f'Job {job_id} failed: {e}')
        mark_job_failed(supabase, job_id, str(e))
        return False


def mark_job_failed(supabase: Client, job_id: str, error_message: str):
    """Mark a job as failed."""
    try:
        supabase.table('worker_jobs').update({
            'status': 'failed',
            'failed_at': datetime.utcnow().isoformat(),
            'error_message': error_message[:500]  # Truncate long errors
        }).eq('id', job_id).execute()
    except Exception as e:
        logger.error(f'Failed to mark job {job_id} as failed: {e}')


def mark_job_skipped(supabase: Client, job_id: str, reason: str):
    """Mark a job as skipped."""
    try:
        supabase.table('worker_jobs').update({
            'status': 'skipped',
            'error_message': reason
        }).eq('id', job_id).execute()
    except Exception as e:
        logger.error(f'Failed to mark job {job_id} as skipped: {e}')


def main():
    """Main worker loop."""
    logger.info('5BY Pack Worker starting...')
    logger.info(f'Worker version: {WORKER_VERSION}')
    logger.info(f'Environment: {os.getenv("RENDER", "local")}')
    logger.info(f'Poll interval: {POLL_INTERVAL}s')
    
    supabase = get_supabase_client()
    
    if not supabase:
        logger.error('Supabase client not initialized. Running in heartbeat-only mode.')
        while True:
            logger.info('Heartbeat (no Supabase connection)')
            time.sleep(30)
        return
    
    logger.info('Supabase client initialized. Starting job polling...')
    
    while True:
        try:
            job = poll_jobs(supabase)
            
            if job:
                job_id = job['id']
                
                # Attempt to claim the job atomically
                if claim_job(supabase, job_id):
                    process_job(supabase, job)
                else:
                    logger.debug(f'Job {job_id} was claimed by another worker')
            else:
                logger.debug('No pending jobs')
            
        except Exception as e:
            logger.error(f'Worker loop error: {e}')
        
        time.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    main()
