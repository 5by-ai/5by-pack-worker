"""
Load Test Script for Local Model
GATE: GATE-201-render-direct-model-runtime-v1

Usage:
  python load_test.py

This script tests:
1. Import success
2. Model load success
3. First load latency
4. Warm inference latency

NOT connected to worker business logic.
"""

import sys
import time
import traceback


def run_load_test():
    results = {
        'import_success': False,
        'model_load_success': False,
        'first_load_latency_ms': None,
        'warm_inference_latency_ms': None,
        'errors': []
    }

    # Step 1: Import test
    print('[1/4] Testing import...')
    try:
        from lib.local_model import ensure_model_loaded, test_inference, get_model_status
        results['import_success'] = True
        print('  ✓ Import success')
    except Exception as e:
        results['errors'].append(f'import_failed: {e}')
        print(f'  ✗ Import failed: {e}')
        traceback.print_exc()
        return results

    # Step 2: Model load test (cold start)
    print('[2/4] Testing model load (cold start)...')
    try:
        start = time.time()
        model, tokenizer = ensure_model_loaded()
        first_load_ms = int((time.time() - start) * 1000)
        results['model_load_success'] = True
        results['first_load_latency_ms'] = first_load_ms
        print(f'  ✓ Model loaded in {first_load_ms}ms')
    except Exception as e:
        results['errors'].append(f'model_load_failed: {e}')
        print(f'  ✗ Model load failed: {e}')
        traceback.print_exc()
        return results

    # Step 3: First inference (warm-up)
    print('[3/4] Testing first inference...')
    try:
        result = test_inference("Summarize: This is a test conversation about AI.")
        print(f'  ✓ First inference: {result["inference_ms"]}ms')
        print(f'    Output: {result["output"][:100]}...')
    except Exception as e:
        results['errors'].append(f'first_inference_failed: {e}')
        print(f'  ✗ First inference failed: {e}')
        traceback.print_exc()

    # Step 4: Warm inference
    print('[4/4] Testing warm inference...')
    try:
        result = test_inference("Summarize: Another test message.")
        results['warm_inference_latency_ms'] = result['inference_ms']
        print(f'  ✓ Warm inference: {result["inference_ms"]}ms')
    except Exception as e:
        results['errors'].append(f'warm_inference_failed: {e}')
        print(f'  ✗ Warm inference failed: {e}')
        traceback.print_exc()

    # Summary
    print('\n' + '='*50)
    print('LOAD TEST RESULTS')
    print('='*50)
    print(f'Import success:        {results["import_success"]}')
    print(f'Model load success:    {results["model_load_success"]}')
    print(f'First load latency:    {results["first_load_latency_ms"]}ms')
    print(f'Warm inference:        {results["warm_inference_latency_ms"]}ms')
    if results['errors']:
        print(f'Errors:                {results["errors"]}')
    print('='*50)

    # Exit code
    if results['model_load_success']:
        print('\n✓ LOAD TEST PASSED')
        return results
    else:
        print('\n✗ LOAD TEST FAILED')
        sys.exit(1)


if __name__ == '__main__':
    run_load_test()
