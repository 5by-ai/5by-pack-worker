#!/usr/bin/env python3
"""
5BY Pack Boundary Worker

Render Standard Background Worker for:
- Post-Pack diagnostics (v1 enabled)
- Shadow baseline comparison (sampling only)
- Queue-based async processing
- #201 P1: flow_change_detection, over_segmentation_check (rule-based)

v1 Constraints:
- NO context_packs modification
- NO raw_messages modification
- NO authoritative Pack decision
- job_type 'pack_admission' is DB-blocked
"""

import os
import time
import logging
from datetime import datetime, timezone
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
WORKER_VERSION = 'v0.2.0'

# Polling interval (seconds)
POLL_INTERVAL = 10


def now_iso_utc() -> str:
    """Return current UTC time as ISO string."""
    return datetime.now(timezone.utc).isoformat()


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
            'picked_at': now_iso_utc(),
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


def mark_job_failed(supabase: Client, job_id: str, error_message: str):
    """Mark a job as failed."""
    try:
        supabase.table('worker_jobs').update({
            'status': 'failed',
            'failed_at': now_iso_utc(),
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


def validate_required_payload_fields(supabase: Client, job_id: str, pack_id: Optional[str], payload: Dict[str, Any]) -> Optional[str]:
    """Validate pack_id and conversation_id. Returns conversation_id or None if invalid."""
    conversation_id = payload.get('conversation_id')
    if not pack_id or not conversation_id:
        error_message = 'invalid_payload: missing pack_id or conversation_id'
        mark_job_failed(supabase, job_id, error_message)
        return None
    return conversation_id


def build_rule_engine_result_namespace(job_type: str, output_type: str, output: Dict[str, Any]) -> Dict[str, Any]:
    """Build SPEC-201 compliant namespace structure for rule_engine_result."""
    created_at = now_iso_utc()
    output_with_ts = dict(output)
    output_with_ts['created_at'] = created_at

    return {
        'contract': 'SPEC-201-runtime-intelligence-output-contract-v1',
        'job_type': job_type,
        'output_type': output_type,
        'output': output_with_ts,
        'diagnostic_only': True,
        'created_at': created_at
    }


def record_rule_engine_diagnostics(
    supabase: Client,
    job_id: str,
    pack_id: Optional[str],
    rule_engine_result: Dict[str, Any]
) -> None:
    """Insert diagnostics with rule_engine_result JSONB only (no shadow_result)."""
    supabase.table('worker_diagnostics').insert({
        'job_id': job_id,
        'pack_id': pack_id,
        'rule_engine_result': rule_engine_result
    }).execute()


def handle_flow_change_detection(supabase: Client, job_id: str, pack_id: Optional[str], payload: Dict[str, Any], logical_pair_id: Optional[str]) -> None:
    """
    #201 P1: flow_change_detection handler (rule-based).
    
    Rules:
    - No previous pair → same_flow (first_pair)
    - Time gap >= 30min → new_flow_candidate
    - Time gap < 30min → unclear
    
    Canonical tables: SELECT-only (no write).
    Output: worker_diagnostics.rule_engine_result JSONB.
    """
    conversation_id = validate_required_payload_fields(supabase, job_id, pack_id, payload)
    if not conversation_id:
        return

    if not logical_pair_id:
        mark_job_failed(supabase, job_id, 'invalid_payload: missing logical_pair_id')
        return

    # SELECT-only: get current pair
    current_pair = supabase.table('logical_pairs').select('id, conversation_id, created_at').eq('id', logical_pair_id).single().execute().data
    if not current_pair or current_pair.get('conversation_id') != conversation_id:
        mark_job_failed(supabase, job_id, 'invalid_payload: logical_pair not found or conversation mismatch')
        return

    current_created_at = current_pair.get('created_at')
    if not current_created_at:
        mark_job_failed(supabase, job_id, 'invalid_payload: logical_pair missing created_at')
        return

    # SELECT-only: get previous pair in same conversation
    prev_pairs = supabase.table('logical_pairs').select('id, created_at').eq('conversation_id', conversation_id).lt('created_at', current_created_at).order('created_at', desc=True).limit(1).execute().data

    signal = 'unclear'
    reason = 'rule: default_unclear'
    confidence = 0.5

    if not prev_pairs:
        signal = 'same_flow'
        reason = 'rule: first_pair'
    else:
        prev_created_at = prev_pairs[0].get('created_at')
        try:
            curr_dt = datetime.fromisoformat(str(current_created_at).replace('Z', '+00:00'))
            prev_dt = datetime.fromisoformat(str(prev_created_at).replace('Z', '+00:00'))
            gap_minutes = (curr_dt - prev_dt).total_seconds() / 60.0
            if gap_minutes >= 30.0:
                signal = 'new_flow_candidate'
                reason = 'rule: time_gap_30min'
            else:
                signal = 'unclear'
                reason = 'rule: time_gap_lt_30min'
        except Exception:
            signal = 'unclear'
            reason = 'rule: created_at_parse_failed'

    rule_engine_result = build_rule_engine_result_namespace(
        job_type='flow_change_detection',
        output_type='flow_change_signal',
        output={
            'type': 'flow_change_signal',
            'pack_id': pack_id,
            'logical_pair_id': logical_pair_id,
            'signal': signal,
            'confidence': confidence,
            'reason': reason
        }
    )

    record_rule_engine_diagnostics(supabase, job_id, pack_id, rule_engine_result)
    logger.info(f'Job {job_id}: flow_change_detection signal={signal}')


def handle_over_segmentation_check(supabase: Client, job_id: str, pack_id: Optional[str], payload: Dict[str, Any]) -> None:
    """
    #201 P1: over_segmentation_check handler (rule-based).
    
    Rules:
    - message_count < 2 → should_suppress=True (too_small)
    - Previous pack created within 5min + message_count < 3 → should_suppress=True (too_recent)
    - Otherwise → should_suppress=False
    
    CRITICAL: should_suppress is diagnostic-only recommendation.
    It does NOT suppress context_pack creation by itself.
    #193 route filter remains the only canonical admission gate.
    
    Canonical tables: SELECT-only (no write).
    Output: worker_diagnostics.rule_engine_result JSONB.
    """
    conversation_id = validate_required_payload_fields(supabase, job_id, pack_id, payload)
    if not conversation_id:
        return

    # SELECT-only: get current pack
    current_pack = supabase.table('context_packs').select('id, conversation_id, created_at, message_count').eq('id', pack_id).single().execute().data
    if not current_pack or current_pack.get('conversation_id') != conversation_id:
        mark_job_failed(supabase, job_id, 'invalid_payload: pack not found or conversation mismatch')
        return

    message_count = current_pack.get('message_count')
    try:
        message_count_int = int(message_count) if message_count is not None else 0
    except Exception:
        message_count_int = 0

    should_suppress = False
    reason_code = 'minor_edit'
    reason = 'rule: default_minor_edit'

    if message_count_int < 2:
        should_suppress = True
        reason_code = 'too_small'
        reason = 'rule: pack_message_count_lt_2'
    else:
        # SELECT-only: check previous pack timing
        packs = supabase.table('context_packs').select('id, created_at').eq('conversation_id', conversation_id).order('created_at', desc=True).limit(2).execute().data
        prev_pack = None
        for p in packs:
            if p.get('id') != pack_id:
                prev_pack = p
                break

        if prev_pack:
            try:
                curr_dt = datetime.fromisoformat(str(current_pack.get('created_at')).replace('Z', '+00:00'))
                prev_dt = datetime.fromisoformat(str(prev_pack.get('created_at')).replace('Z', '+00:00'))
                gap_minutes = (curr_dt - prev_dt).total_seconds() / 60.0
                if gap_minutes <= 5.0 and message_count_int < 3:
                    should_suppress = True
                    reason_code = 'too_recent'
                    reason = 'rule: pack_created_within_5min_and_message_count_lt_3'
            except Exception:
                pass

    rule_engine_result = build_rule_engine_result_namespace(
        job_type='over_segmentation_check',
        output_type='over_segmentation_signal',
        output={
            'type': 'over_segmentation_signal',
            'pack_id': pack_id,
            'should_suppress': should_suppress,
            'reason_code': reason_code,
            'reason': reason
        }
    )

    record_rule_engine_diagnostics(supabase, job_id, pack_id, rule_engine_result)
    logger.info(f'Job {job_id}: over_segmentation_check should_suppress={should_suppress}')


def process_job(supabase: Client, job: Dict[str, Any]) -> bool:
    """
    Process a job based on job_type.
    v1: post_pack_diagnostics, flow_change_detection, over_segmentation_check enabled.
    
    Returns True if successful, False otherwise.
    """
    job_id = job['id']
    job_type = job['job_type']
    pack_id = job.get('pack_id')
    payload = job.get('payload', {})
    logical_pair_id = job.get('logical_pair_id')
    
    logger.info(f'Processing job {job_id} type={job_type} pack_id={pack_id}')
    
    start_time = time.time()
    
    try:
        if job_type == 'post_pack_diagnostics':
            # v1: Record diagnostics only (no Pack modification)
            rule_engine_result = payload.get('cheap_signals', {})
            record_rule_engine_diagnostics(supabase, job_id, pack_id, rule_engine_result)
            logger.info(f'Job {job_id}: diagnostics recorded')
            
        elif job_type == 'flow_change_detection':
            # #201 P1: flow change detection (rule-based)
            handle_flow_change_detection(supabase, job_id, pack_id, payload, logical_pair_id)
            
        elif job_type == 'over_segmentation_check':
            # #201 P1: over segmentation check (rule-based)
            handle_over_segmentation_check(supabase, job_id, pack_id, payload)
            
        elif job_type == 'shadow_baseline':
            # v1: Sampling only - just log for now
            logger.info(f'Job {job_id}: shadow_baseline (sampling mode, no-op)')
            
        else:
            # Future gated job types (under_segmentation_check, provisional_enrichment, expensive_handoff_eval)
            logger.warning(f'Job {job_id}: job_type {job_type} not enabled in v1, skipping')
            mark_job_skipped(supabase, job_id, f'{job_type} not enabled in v1')
            return True
        
        # Mark job as completed
        latency_ms = int((time.time() - start_time) * 1000)
        supabase.table('worker_jobs').update({
            'status': 'completed',
            'completed_at': now_iso_utc()
        }).eq('id', job_id).execute()
        
        logger.info(f'Job {job_id} completed in {latency_ms}ms')
        return True
        
    except Exception as e:
        logger.error(f'Job {job_id} failed: {e}')
        mark_job_failed(supabase, job_id, str(e))
        return False


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
