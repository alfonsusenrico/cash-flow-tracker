#!/usr/bin/env python3
"""
Benchmark script to evaluate free vision/multimodal models on OpenRouter
for receipt and invoice text extraction.
"""

import argparse
import base64
import json
import mimetypes
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

# Default list of free vision models to use as fallback
FALLBACK_MODELS = [
    "google/gemma-4-31b-it:free",
    "google/gemma-4-26b-a4b-it:free",
    "nvidia/nemotron-nano-12b-v2-vl:free",
    "nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free",
    "moonshotai/kimi-k2.6:free",
]

# Standard production prompt
EXTRACTION_PROMPT = (
    "You are a receipt/invoice parser. Extract all relevant details from this image "
    "as plain text. Include store name, date, items, prices, total amount, taxes, "
    "and any other visible text. Do not summarize or omit numbers; write down "
    "everything clearly."
)


def load_env_file(env_path: Path) -> Dict[str, str]:
    """Manually parse a .env file into a dictionary."""
    env_vars = {}
    if not env_path.exists():
        return env_vars
    
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip("'\"")
                env_vars[key] = val
    return env_vars


def get_api_key() -> Optional[str]:
    """Find the OpenRouter/DeepSeek API key from environment or .env files."""
    # 1. Check current system environment
    api_key = os.environ.get("DEEPSEEK_API_KEY") or os.environ.get("OPENROUTER_API_KEY")
    if api_key:
        return api_key

    # 2. Search for .env files in likely locations
    search_paths = [
        Path.cwd() / ".env",
        Path.cwd().parent / ".env",
        Path(__file__).resolve().parent / ".env",
        Path(__file__).resolve().parent.parent / ".env",
    ]

    for path in search_paths:
        if path.exists():
            env_vars = load_env_file(path)
            key = env_vars.get("DEEPSEEK_API_KEY") or env_vars.get("OPENROUTER_API_KEY")
            if key:
                return key

    return None


def discover_free_vision_models(api_key: str) -> List[str]:
    """Query OpenRouter API to discover free vision models."""
    url = "https://openrouter.ai/api/v1/models"
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    
    print("Fetching active models from OpenRouter API...")
    try:
        with httpx.Client(timeout=20.0) as client:
            resp = client.get(url, headers=headers)
            resp.raise_for_status()
            models_data = resp.json().get("data", [])
            
        discovered = []
        for m in models_data:
            model_id = m.get("id", "")
            pricing = m.get("pricing", {})
            
            # Check if pricing is 0 for both prompt and completion
            try:
                prompt_price = float(pricing.get("prompt", 0))
                completion_price = float(pricing.get("completion", 0))
            except (ValueError, TypeError):
                prompt_price = 1.0
                completion_price = 1.0
                
            is_free = (prompt_price == 0.0 and completion_price == 0.0) or model_id.endswith(":free")
            
            # Check if it supports image input modality
            arch = m.get("architecture", {})
            input_modalities = arch.get("input_modalities", [])
            modality = arch.get("modality", "")
            has_image = "image" in input_modalities or "image" in modality
            
            # Filter out non-general purpose models or content safety models
            is_safety = "content-safety" in model_id or "moderation" in model_id
            is_generic_stub = model_id == "openrouter/free"
            
            if is_free and has_image and not is_safety and not is_generic_stub:
                discovered.append(model_id)
                
        return sorted(list(set(discovered)))
    except Exception as e:
        print(f"⚠️ Warning: Auto-discovery failed ({e}). Falling back to hardcoded free vision model list.")
        return FALLBACK_MODELS


def encode_image(image_path: Path) -> Tuple[str, str]:
    """Read local image and return its base64 encoded string and MIME type."""
    if not image_path.exists():
        raise FileNotFoundError(f"Image not found at: {image_path}")
        
    mime_type, _ = mimetypes.guess_type(str(image_path))
    if not mime_type:
        mime_type = "image/jpeg"
        
    with open(image_path, "rb") as f:
        img_bytes = f.read()
        
    b64_str = base64.b64encode(img_bytes).decode("utf-8")
    return b64_str, mime_type


def run_model_benchmark(
    client: httpx.Client,
    model_id: str,
    api_key: str,
    b64_image: str,
    mime_type: str,
    timeout: int,
) -> Dict[str, Any]:
    """Run a single chat completion request to OpenRouter for a specific model."""
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/alfonsusenrico/cash-flow-tracker",
        "X-Title": "Cash Flow Tracker Receipt Benchmark",
    }
    
    payload = {
        "model": model_id,
        "temperature": 0.0,
        "messages": [
            {
                "role": "system",
                "content": "You are a helpful assistant that extracts text from images.",
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": EXTRACTION_PROMPT},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{mime_type};base64,{b64_image}"},
                    },
                ],
            },
        ],
    }
    
    start_time = time.perf_counter()
    try:
        resp = client.post(url, json=payload, headers=headers, timeout=float(timeout))
        latency = time.perf_counter() - start_time
        
        if resp.status_code == 200:
            resp_json = resp.json()
            choices = resp_json.get("choices", [])
            if choices:
                text = choices[0].get("message", {}).get("content", "")
                return {
                    "status": "OK",
                    "latency": latency,
                    "text": text,
                    "error": None,
                }
            else:
                return {
                    "status": "FAIL",
                    "latency": latency,
                    "text": "",
                    "error": f"Empty choices in response: {resp.text}",
                }
        else:
            return {
                "status": "FAIL",
                "latency": latency,
                "text": "",
                "error": f"HTTP {resp.status_code}: {resp.text}",
            }
            
    except httpx.TimeoutException:
        latency = time.perf_counter() - start_time
        return {
            "status": "TIMEOUT",
            "latency": latency,
            "text": "",
            "error": f"Request timed out after {timeout}s",
        }
    except Exception as e:
        latency = time.perf_counter() - start_time
        return {
            "status": "ERROR",
            "latency": latency,
            "text": "",
            "error": str(e),
        }


def print_table(results: List[Dict[str, Any]]) -> None:
    """Print results in a beautiful, formatted console table."""
    # Headers and widths
    # Rank (4), Model ID (45), Avg Latency (10), Status (8), Output Length (12)
    col_widths = [4, 48, 10, 8, 13]
    
    def border(left, middle, right):
        parts = [col_widths[i] * "─" for i in range(len(col_widths))]
        return left + middle.join(parts) + right

    # Top border
    print(border("┌", "┬", "┐"))
    
    # Header row
    headers = ["Rank", "Model ID", "Avg Time", "Status", "Output Size"]
    header_str = "│"
    for i, h in enumerate(headers):
        header_str += f" {h:<{col_widths[i]-2}} │"
    print(header_str)
    
    # Separator
    print(border("├", "┼", "┤"))
    
    # Row printing
    for idx, r in enumerate(results, 1):
        rank = str(idx)
        model = r["model"]
        
        status = r["status"]
        if status == "OK":
            status_str = "✅ OK"
        elif status == "TIMEOUT":
            status_str = "⏳ T/O"
        else:
            status_str = "❌ FAIL"
            
        latency = f"{r['avg_latency']:.2f}s" if r["avg_latency"] is not None else "—"
        length = f"{r['avg_length']} chars" if r["status"] == "OK" else "—"
        
        row_str = f"│ {rank:<{col_widths[0]-2}} │ {model:<{col_widths[1]-2}} │ {latency:<{col_widths[2]-2}} │ {status_str:<{col_widths[3]-2}} │ {length:<{col_widths[4]-2}} │"
        print(row_str)
        
    # Bottom border
    print(border("└", "┴", "┘"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark free vision models on OpenRouter for receipt OCR/extraction."
    )
    parser.add_argument(
        "--image",
        type=str,
        default="telegram-bot/test_images/sample_receipt.jpg",
        help="Path to the test receipt image file",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="OpenRouter API key (defaults to DEEPSEEK_API_KEY from environment/.env)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="Timeout in seconds per request (default: 120)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="Number of runs per model for averaging latency (default: 1)",
    )
    parser.add_argument(
        "--models",
        type=str,
        default=None,
        help="Comma-separated list of specific model IDs to test (skips auto-discovery)",
    )
    
    args = parser.parse_args()
    
    # Find API key
    api_key = args.api_key or get_api_key()
    if not api_key:
        print("❌ Error: OpenRouter API key not found. Please provide via --api-key or set DEEPSEEK_API_KEY in environment or .env file.")
        return 1
        
    # Find Image
    image_path = Path(args.image)
    if not image_path.exists():
        # Check if run from within telegram-bot/ directory
        alt_path = Path("test_images") / image_path.name
        if alt_path.exists():
            image_path = alt_path
        else:
            print(f"❌ Error: Image file not found at '{args.image}' or '{alt_path}'")
            return 1
            
    print(f"Using test image: {image_path} ({image_path.stat().st_size / 1024:.1f} KB)")
    
    # Select models
    if args.models:
        models_to_test = [m.strip() for m in args.models.split(",") if m.strip()]
        print(f"Benchmarking {len(models_to_test)} specified models...")
    else:
        print("Auto-discovering free vision models on OpenRouter...")
        models_to_test = discover_free_vision_models(api_key)
        print(f"Discovered {len(models_to_test)} free vision models to benchmark.")
        
    if not models_to_test:
        print("❌ Error: No models to test.")
        return 1
        
    # Encode receipt image
    try:
        b64_image, mime_type = encode_image(image_path)
    except Exception as e:
        print(f"❌ Error encoding image: {e}")
        return 1
        
    print(f"MIME type detected: {mime_type}")
    print(f"Prompt: {EXTRACTION_PROMPT}\n")
    
    benchmark_results = []
    
    with httpx.Client() as client:
        for idx, model in enumerate(models_to_test, 1):
            print(f"[{idx}/{len(models_to_test)}] Testing model: {model}")
            
            runs_data = []
            failures = 0
            timeouts = 0
            successes = 0
            latencies = []
            lengths = []
            last_text = ""
            last_error = None
            
            for run in range(1, args.runs + 1):
                if args.runs > 1:
                    print(f"  Run {run}/{args.runs}...", end="", flush=True)
                else:
                    print("  Sending request...", end="", flush=True)
                    
                result = run_model_benchmark(
                    client,
                    model,
                    api_key,
                    b64_image,
                    mime_type,
                    args.timeout,
                )
                
                runs_data.append(result)
                
                if result["status"] == "OK":
                    successes += 1
                    latencies.append(result["latency"])
                    lengths.append(len(result["text"]))
                    last_text = result["text"]
                    print(f" OK ({result['latency']:.2f}s, {len(result['text'])} chars)")
                elif result["status"] == "TIMEOUT":
                    timeouts += 1
                    last_error = result["error"]
                    print(f" TIMEOUT ({result['latency']:.2f}s)")
                else:
                    failures += 1
                    last_error = result["error"]
                    print(f" FAIL ({result['latency']:.2f}s): {result['error']}")
                    
                # Small rate-limit protection between runs
                if args.runs > 1 and run < args.runs:
                    time.sleep(2.0)
                    
            # Compute stats
            overall_status = "OK" if successes > 0 else ("TIMEOUT" if timeouts > failures else "FAIL")
            avg_latency = sum(latencies) / len(latencies) if latencies else None
            avg_length = int(sum(lengths) / len(lengths)) if lengths else 0
            
            benchmark_results.append({
                "model": model,
                "status": overall_status,
                "avg_latency": avg_latency,
                "avg_length": avg_length,
                "success_rate": f"{successes}/{args.runs}",
                "last_text": last_text,
                "last_error": last_error,
                "runs": runs_data,
            })
            
            # Rate-limit protection between models
            time.sleep(3.0)
            
    # Sort results:
    # 1. OK models sorted by average latency ascending
    # 2. TIMEOUT models
    # 3. FAIL models
    def sort_key(r):
        if r["status"] == "OK":
            return (0, r["avg_latency"])
        elif r["status"] == "TIMEOUT":
            return (1, r["avg_latency"] or 9999)
        else:
            return (2, r["avg_latency"] or 9999)
            
    benchmark_results.sort(key=sort_key)
    
    print("\n" + "="*30 + " BENCHMARK SUMMARY " + "="*30)
    print_table(benchmark_results)
    
    # Save detailed results to JSON
    timestamp = int(time.time())
    output_filename = f"benchmark_results_{timestamp}.json"
    output_path = Path("telegram-bot") / output_filename
    if not output_path.parent.exists():
        output_path = Path(output_filename)
        
    report = {
        "timestamp": timestamp,
        "test_image": str(image_path),
        "extraction_prompt": EXTRACTION_PROMPT,
        "runs_configured": args.runs,
        "timeout_configured": args.timeout,
        "rankings": [
            {
                "model": r["model"],
                "status": r["status"],
                "avg_latency": r["avg_latency"],
                "avg_length": r["avg_length"],
                "success_rate": r["success_rate"],
                "last_error": r["last_error"],
                "output_preview": r["last_text"][:200] + ("..." if len(r["last_text"]) > 200 else "") if r["status"] == "OK" else None,
            }
            for r in benchmark_results
        ],
        "detailed_results": benchmark_results,
    }
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        
    print(f"\nDetailed report saved to: {output_path.resolve()}")
    
    # Show text preview for the top successful model
    top_ok = next((r for r in benchmark_results if r["status"] == "OK"), None)
    if top_ok:
        print("\n" + "="*30 + f" TOP MODEL EXTRACTED TEXT PREVIEW ({top_ok['model']}) " + "="*30)
        print(top_ok["last_text"])
        print("="*80)
        
    return 0


if __name__ == "__main__":
    sys.exit(main())
