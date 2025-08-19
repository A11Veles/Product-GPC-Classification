from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
import torch, os

model_id = "tiiuae/falcon-7b" 

# Pick compute dtype for GEMMs (weights stay 4-bit)
supports_bf16 = torch.cuda.is_available() and torch.cuda.get_device_capability(0)[0] >= 8
compute_dtype = torch.bfloat16 if supports_bf16 else torch.float16

tokenizer = AutoTokenizer.from_pretrained(
    model_id,
    use_fast=True,
    trust_remote_code=True  # Falcon uses custom code
)

if tokenizer.pad_token is None:
    tokenizer.pad_token = tokenizer.eos_token

if torch.cuda.is_available():
    # --- GPU path: 4-bit weight-only quantization at LOAD TIME ---
    bnb_cfg = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_use_double_quant=True,
        bnb_4bit_compute_dtype=compute_dtype,
    )

    model = AutoModelForCausalLM.from_pretrained(
        model_id,
        device_map="auto",
        quantization_config=bnb_cfg,      # ← triggers runtime 4-bit quantization
        torch_dtype=compute_dtype,        # compute dtype only; weights are 4-bit
        trust_remote_code=True,
        # Optional memory controls (tune for your box):
        # max_memory={0: "10GiB", "cpu": "30GiB"},
        # offload_state_dict=True,
        # offload_folder="offload",
    )

    # Optional: choose attention backend
    try:
        model.config.attn_implementation = "flash_attention_2"
    except Exception:
        model.config.attn_implementation = "sdpa"

else:
    # --- CPU fallback: load FP32, then (optional) dynamic quant for Linear ops ---
    model = AutoModelForCausalLM.from_pretrained(
        model_id,
        torch_dtype=torch.float32,
        low_cpu_mem_usage=True,
        trust_remote_code=True,
    ).eval()

    # Optional CPU quantization (weight-only int8 for Linear layers)
    try:
        from torch.ao.quantization import quantize_dynamic
        model = quantize_dynamic(model, {torch.nn.Linear}, dtype=torch.qint8).eval()
        print("Applied CPU dynamic quantization (int8) to Linear layers.")
    except Exception as e:
        print("CPU dynamic quantization not applied:", e)

model.eval()
print("Model loaded.")