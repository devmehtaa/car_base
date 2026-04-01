import fitz
import re
import io
import json
import time
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload


FOLDER_ID = "1J22Hv9BJD5AoB-jCepMMQgEriM-eIVnq"
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
CREDS_PATH = r"C:\Users\phadt\Documents\IST440W-main\textExtraction\credentials.json"
OUTPUT_FILE = "structured_results.json"


OIL_PATTERN = r"\b(0|5|10|15|20|25)\s*W[-–—]?\s*(16|20|30|40|50|60)\b"
CAPACITY_PATTERN = r"(\d+\.?\d*)\s*(?:us\s+|imp\s+|u\.s\.\s+)?(quarts?|qts?|qt\.?|liters?|l\b)"
ENGINE_PATTERN = r"\b([1-6]\.\d)\s*[-]?\s*(l|liter|litre)\b"
ENGINE_TYPE_PATTERN = r"\b(v6|v8|v10|v12|v16|i3|i4|i5|i6|i8|h4|w8|w12|w16|f8|flat\s*6|boxe|boxer|turbo|supercharged|twin-turbo)\b"
TEMP_PATTERN = r"(-?\d+)\s*°?\s*(c|f)"

NON_ENGINE_CONTEXT = [
    "brake", "transmission", "gear oil", "power steering",
    "differential", "coolant", "washer", "clutch", "mtf",
    "manual transmission fluid", "temporary replacement", "filler bolt",
    "synchronizer", "gearbox fluid", "transmission fluid", "atf",
    "automatic transmission", "fluid level", "fluid change"
]

INVALID_WORDS = [
    "motor", "motors", "company", "co", "ltd", "inc", "manual", "owner"
]

KNOWN_MAKES = [
    "honda", "toyota", "nissan", "mazda", "subaru", "mitsubishi", "suzuki", "daihatsu",
    "lexus", "acura", "infiniti", "yamaha",
    "bmw", "mercedes", "audi", "volkswagen", "vw", "porsche", "lamborghini", "bugatti",
    "maybach", "trabant",
    "ford", "chevrolet", "chevy", "dodge", "jeep", "ram", "gmc", "cadillac", "buick",
    "oldsmobile", "pontiac", "hummer", "tesla",
    "volvo", "saab", "jaguar", "land rover", "landrover", "bentley", "rolls royce", "aston martin",
    "lotus", "mclaren", "ferrar", "lamborghini", "maserati", "alfa romeo", "fiat", "lancia",
    "peugeot", "citroen", "renault", "dacia", "rally", "seat", "skoda",
    "hyundai", "kia", "daewoo", "ssangyong",
    "geely", "byd", "chery", "great wall", "haval", "changan", "jac", "brilliance",
    "lifan", "saic", "gac", "xpeng", "nio", "li auto",
    "mg", "morris", "austin", "british leyland", "rover",
    "megene", "baic",
    "lada", "moskvitch", "gaz", "kamaz",
    "tata", "mahindra", "hero", "maruti",
    "proton", "perodua",
    "isuzu",
    "ferrari", "pagani",
    "koenigsegg",
    "koenigsegg", "pagani", "bugatti", "rimac", "hennessey"
]

BODY_TYPES = ["hatchback", "sedan", "coupe", "wagon", "suv", "truck"]

ENGINE_CODE_MAP = {
    "d13": 1.3, "d15": 1.5, "d16": 1.6, "d17": 1.7, "b16": 1.6, "b18": 1.8, "b20": 2.0,
    "f22": 2.2, "h22": 2.2, "h23": 2.3, "k20": 2.0, "k24": 2.4, "r18": 1.8, "l13": 1.3, "l15": 1.5,
    "j35": 3.5, "j30": 3.0, "j32": 3.2,
    "1nz": 1.5, "2nz": 1.3, "1zz": 1.8, "2zz": 1.8, "1mz": 3.0, "2az": 2.4, "3s": 2.0,
    "4afe": 1.6, "4age": 1.6, "5sfe": 2.2, "5vz": 3.4, "2jz": 3.0, "1jz": 2.5, "7mge": 3.0,
    "1uz": 4.0, "3uz": 4.3, "4gr": 2.5, "5gr": 2.5,
    "l3": 2.3, "l5": 2.5, "ej20": 2.0, "ej25": 2.5, "fb20": 2.0, "fb25": 2.5, "fa20": 2.0,
    "ej18": 1.8, "ej15": 1.5, "fa18": 1.8, "z14": 1.4, "z18": 1.8,
    "i4": 2.0, "duratec": 2.3, "ecoboost": 2.0, "triton": 5.4, "modular": 4.6, "cologne": 2.8,
    "zetec": 2.0, "duratech": 2.3, "cvh": 1.6, "vulcan": 3.0, "ranger": 2.3, "boss": 7.0,
    "ls": 5.3, "lt": 5.3, "vortec": 5.3, "smallblock": 5.7, "350": 5.7, "454": 7.4,
    "ecotec": 2.0, "i4": 2.4, "v6": 3.8, "3800": 3.8, "liter": 2.0, "ohc": 2.2, "ling": 3.6,
    "4.8l": 4.8, "5.0l": 5.0, "6.0l": 6.0, "v8": 5.7,
    "ka": 1.6, "qr": 1.6, "sr20": 2.0, "sr18": 1.8, "vq": 3.5, "vg": 2.8, "rb": 2.6,
    "z": 3.0, "ga16": 1.6, "qg16": 1.6, "qg18": 1.8, "vq37": 3.7, "vq35": 3.5,
    "n51": 3.0, "n52": 3.0, "n55": 3.0, "n63": 4.4, "m54": 3.0, "m20": 2.0,
    "m52": 2.8, "s54": 3.2, "m50": 2.5, "m30": 3.5, "i6": 3.0, "s65": 4.0, "n74": 4.4,
    "m113": 5.0, "m156": 6.2, "m157": 5.5, "m111": 2.3, "m112": 3.2, "m104": 3.2,
    "om606": 3.0, "om642": 3.0, "w8": 4.0, "v12": 6.0, "m135": 3.5, "m272": 3.5,
    "aeb": 1.8, "aba": 2.0, "ajz": 2.0, "ala": 1.8, "alf": 2.0, "amc": 1.9,
    "bwe": 1.9, "amk": 1.9, "brr": 2.0, "bpy": 2.0, "ccta": 2.0, "aaa": 3.2,
    "aam": 4.2, "abd": 5.0, "aea": 5.0,
    "g4": 1.4, "g4nj": 1.4, "g4fj": 1.4, "g4fa": 1.4, "g5pl": 1.5, "g6ba": 1.6,
    "g6da": 1.6, "v6": 2.0, "d4cb": 2.5, "d4bh": 2.5, "d4ha": 1.5, "g4aj": 1.3,
    "g8ba": 2.0, "d5ea": 2.0,
    "318": 5.2, "340": 5.6, "360": 5.9, "383": 6.3, "426": 7.0, "440": 7.2,
    "5.7l": 5.7, "5.9l": 5.9, "6.4l": 6.4, "3.7l": 3.7, "4.7l": 4.7, "4.0l": 4.0,
    "2.0l": 2.0, "2.4l": 2.4, "3.0l": 3.0, "magnum": 5.7, "hemi": 5.7,
    "b5204": 2.0, "b5254": 2.5, "b5244": 2.4, "b6304": 3.0, "b6294": 2.9,
    "b204": 2.0, "b234": 2.3, "b235": 2.3, "b205": 2.0, "b308": 3.0, "b309": 2.3,
    "aj6": 3.6, "aj16": 4.0, "aj26": 4.0, "v12": 6.0,
    "m57": 3.0, "n62": 4.4, "tdi": 2.7,
    "169a2": 1.7, "329a4": 2.0, "4c41": 1.7, "ty100": 1.0,
    "tu3m": 1.4, "tu5m": 1.6, "ew10": 2.0, "bf6": 1.6,
    "d4f": 1.6, "k4m": 1.6, "m9m": 1.5,
    "4a90": 1.5, "4a91": 1.5, "4g64": 2.4, "6a13": 1.6, "6g74": 3.0,
    "g10": 1.0, "g13": 1.3, "g16": 1.6, "h25": 2.5, "m15": 1.5,
    "cb23": 2.3, "cb90": 0.9, "ef": 1.3,
    "4ze1": 1.8, "4zc1": 2.2, "4jg2": 1.8, "6sd1": 3.0,
    "a15": 1.5, "a17": 1.7, "x16": 1.6, "x18": 1.8,
    "664": 2.3, "665": 2.3, "c20sed": 2.0,
    "4g64": 1.5, "4g69": 1.5, "4g94": 1.8, "jl4g15": 1.5, "jl4g18": 1.8,
    "jl4g15": 1.5, "jl4g18": 1.8, "jl371qa": 1.8, "3at": 1.6,
    "sqr372": 1.3, "sqr4g15": 1.5, "sqr4g18": 1.8, "sqr4g64": 2.4,
    "cc6460": 2.2, "cc6461": 2.2, "cc6480": 2.4, "cc6472": 2.0,
    "ca4g47": 1.9, "cja1": 1.6, "sc4d20": 2.0,
    "ba3": 1.6, "z18xer": 1.8, "z16se": 1.6,
    "sdi21": 2.2, "dv4": 1.4, "ct100": 1.0,
    "m2dicr": 2.2, "tc60": 1.6,
    "db7": 3.2, "v12": 6.0, "v8": 4.3,
    "f430": 4.3, "f355": 3.6, "f512": 5.1,
    "twin": 6.8, "w12": 6.0,
    "ca": 3.2, "ma": 3.6, "m97": 3.6, "m96": 3.2, "m96a": 3.2,
    "l7": 3.5, "l8": 4.0,
    "v8": 4.2, "v12": 4.9,
}

OIL_PAGE_KEYWORDS = [
    "oil capacity", "engine oil", "crankcase", "oil with filter",
    "oil change capacity", "including filter", "engine oil capacity",
    "engine oil recommendation", "viscosity", "api service",
    "lubricant", "specifications", "technical information"
]


def get_drive_service():
    """
    Step 1: Authenticate with Google Drive
    Loads service account credentials and returns an authenticated Drive API client.
    This allows reading files from Google Drive folders.
    """
    creds = service_account.Credentials.from_service_account_file(
        CREDS_PATH, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)


def get_all_pdfs(service, folder_id):
    """
    Step 2: Recursively search Google Drive folder for PDF files
    Traverses all subfolders and collects PDF files.
    
    Args:
        service: Authenticated Google Drive API client
        folder_id: Root folder ID to search
    
    Returns:
        List of PDF file objects with id, name, mimeType
    """
    pdfs, folders = [], [folder_id]
    
    while folders:
        current = folders.pop()
        response = service.files().list(
            q=f"'{current}' in parents and trashed=false",
            fields="files(id,name,mimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        for item in response.get("files", []):
            if item["mimeType"] == "application/vnd.google-apps.folder":
                folders.append(item["id"])
            elif item["mimeType"] == "application/pdf":
                pdfs.append(item)
    
    return pdfs


def download_pdf(service, file_id):
    """
    Step 3: Download PDF file from Google Drive
    Retrieves file content as binary stream.
    
    Args:
        service: Authenticated Google Drive API client
        file_id: Google Drive file ID
    
    Returns:
        BytesIO buffer containing PDF data
    """
    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    
    while not done:
        _, done = downloader.next_chunk()
    
    buffer.seek(0)
    return buffer


def clean_text(text):
    """
    Clean PDF text by normalizing whitespace and line breaks.
    Converts multiple spaces/newlines to single spaces for uniform parsing.
    
    Args:
        text: Raw PDF extracted text
    
    Returns:
        Cleaned text string
    """
    return re.sub(r"\s+", " ", text.replace("\n", " "))


def normalize_oil(raw):
    """
    Standardize oil type format (e.g., "5w-30" becomes "5W-30").
    Handles different dash styles and ensures consistent spacing.
    
    Args:
        raw: Raw oil type string from PDF
    
    Returns:
        Normalized oil type string
    """
    oil = raw.upper().replace("–", "-").replace("—", "-")
    oil = re.sub(r"\s+", "", oil)
    if "W-" not in oil:
        oil = oil.replace("W", "W-")
    return oil


def to_quarts_liters(value, unit):
    """
    Convert capacity value to both quarts and liters.
    Handles imperial (quarts) to metric (liters) conversion and vice versa.
    
    Args:
        value: Numeric capacity value
        unit: Unit type (quarts, qts, liters, l, etc.)
    
    Returns:
        Tuple of (quarts, liters)
    """
    value = float(value)
    if unit.lower().startswith("l"):
        return round(value / 0.946, 2), value
    else:
        return value, round(value * 0.946, 1)



def parse_filename(name):
    """
    Extract vehicle info from PDF filename format.
    Expected format: YYYY-Make-Model.pdf (e.g., 2017-Honda-Civic.pdf)
    
    Args:
        name: Filename string
    
    Returns:
        Tuple of (year, make, model) or (None, None, None) if parsing fails
    """
    match = re.match(r"(\d{4})-([^-]+)-(.+)\.pdf", name, re.I)
    if match:
        year, make, model = match.groups()
        model = model.replace("-OM", "").replace("-UG", "")
        return int(year), make.capitalize(), model.capitalize()
    return None, None, None


def build_multi_engine_data(engine_caps, oil_scores, oil_temps, engine_oil_map):
    """
    Build structured oil recommendations for each engine in vehicle.
    Calculates per-engine primary/secondary oil selection based on scoring.
    
    Step-by-step process:
    1. Iterate through each detected engine size
    2. Get valid oils from proximity mapping or size-based assignment
    3. Calculate best-scoring oil for THIS engine (not globally)
    4. Mark high-scoring oils as primary/secondary
    5. Include oils with actual temperature conditions
    6. Build complete engine data structure with capacities
    
    Args:
        engine_caps: Dict of engine sizes to capacity data
        oil_scores: Dict of oils to recommendation scores
        oil_temps: Dict of oils to temperature sets
        engine_oil_map: Dict of engines to nearby oils
    
    Returns:
        Dict with engine-to-recommendations structure
    """
    engine_data = {}

    for eng, cap in engine_caps.items():
        with_filter = cap.get("with_filter")
        without_filter = cap.get("without_filter")
        oil_list = []

        if oil_scores:
            valid_oils = engine_oil_map.get(eng, [])
            
            if not valid_oils:
                eng_size = float(eng.replace("L", "")) if eng != "unknown_engine" else 0
                
                if eng_size > 0:
                    for oil in oil_scores.keys():
                        if eng_size <= 1.6 and "40" in oil:
                            valid_oils.append(oil)
                        elif eng_size >= 2.0 and "20" in oil:
                            valid_oils.append(oil)
                else:
                    valid_oils = list(oil_scores.keys())

            if valid_oils:
                engine_best = max(valid_oils, key=lambda oil: oil_scores.get(oil, 0))
                engine_max_score = oil_scores.get(engine_best, 0)
            else:
                engine_best = max(oil_scores, key=oil_scores.get)
                engine_max_score = oil_scores[engine_best]
                valid_oils = list(oil_scores.keys())

            for oil in valid_oils:
                score = oil_scores.get(oil, 0)
                temps = oil_temps.get(oil, [])
                
                has_actual_temps = any(
                    ("°F" in t or "°C" in t or "F" in t or "C" in t or 
                     "above" in t or "below" in t or "range:" in t or 
                     "weather" in t or "temperatures" in t or "cold" in t or "hot" in t)
                    for t in temps
                )

                if score >= engine_max_score - 1 or has_actual_temps:
                    oil_list.append({
                        "oil_type": oil,
                        "recommendation_level": "primary" if oil == engine_best else "secondary",
                        "temperature_condition": list(temps),
                    })

        engine_data[eng] = {
            "oil_capacity": {
                "with_filter": with_filter,
                "without_filter": without_filter
            },
            "oil_recommendations": oil_list
        }

    return engine_data


def pair_quarts_liters(cap_list):
    """
    Pair capacity values by matching quarts with corresponding liters.
    Assumes alternating quarts/liters entries in list.
    
    Step-by-step process:
    1. Iterate through capacity list in pairs
    2. Match quarts value with following liters value
    3. Skip unpaired entries
    4. Return list of {"quarts": X, "liters": Y} objects
    
    Args:
        cap_list: List of capacity objects with quarts/liters fields
    
    Returns:
        List of paired capacity objects
    """
    paired = []
    i = 0
    
    while i < len(cap_list) - 1:
        q = cap_list[i]
        l = cap_list[i + 1]

        if q["quarts"] and l["liters"]:
            paired.append({
                "quarts": q["quarts"],
                "liters": l["liters"]
            })
            i += 2
        else:
            i += 1

    return paired


def detect_vehicle_from_pdf(doc):
    """
    Extract vehicle year, make, model from first few PDF pages.
    Falls back to filename parsing when PDF extraction fails.
    
    Step-by-step process:
    1. Extract text from first 5 pages
    2. Search for 4-digit year (1900-2099)
    3. Count occurrences of known vehicle makes
    4. Find body type (hatchback, sedan, etc.)
    5. Build model name near body type
    6. Fallback to word pair detection for make/model
    
    Args:
        doc: PyMuPDF document object
    
    Returns:
        Tuple of (year, make, model)
    """
    text = ""
    for i in range(min(5, len(doc))):
        text += doc[i].get_text()
    
    text_lower = text.lower()
    words = re.findall(r"[a-z]{3,}", text_lower)
    
    year_match = re.search(r"(19|20)\d{2}", text_lower)
    year = int(year_match.group()) if year_match else None
    
    make_counts = {}
    for word in words:
        if word in KNOWN_MAKES:
            make_counts[word] = make_counts.get(word, 0) + 1
    
    make = max(make_counts, key=make_counts.get).capitalize() if make_counts else None
    
    model = None
    body_index = None
    for i, word in enumerate(words):
        if word in BODY_TYPES:
            body_index = i
            break
    
    if body_index is not None:
        body = words[body_index].capitalize()
        for j in range(body_index - 1, max(body_index - 4, -1), -1):
            candidate = words[j]
            if candidate in INVALID_WORDS or candidate in KNOWN_MAKES:
                continue
            model = f"{candidate.capitalize()} {body}"
            break
    
    if not model:
        for i in range(len(words) - 1):
            w1, w2 = words[i], words[i + 1]
            if w1 in INVALID_WORDS or w2 in INVALID_WORDS:
                continue
            if not make:
                make = w1.capitalize()
            model = w2.capitalize()
            break
    
    return year, make, model


def map_oils_to_engines(text):
    """
    Associate oils with nearby engines based on text proximity.
    Searches 200 chars before and 300 chars after each engine mention.
    
    Step-by-step process:
    1. Find all engine size mentions in text with positions
    2. For each engine, extract text window around it
    3. Find oils in that window
    4. Store list of oils per engine
    
    Args:
        text: Full PDF text
    
    Returns:
        Dict mapping engine sizes to lists of nearby oils
    """
    engine_oil_map = {}
    engine_positions = []
    
    for m in re.finditer(ENGINE_PATTERN, text):
        eng = f"{float(m.group(1)):.1f}L"
        engine_positions.append((eng, m.start()))

    for eng, eng_pos in engine_positions:
        window = text[max(0, eng_pos - 200): eng_pos + 300]
        oils = re.findall(OIL_PATTERN, window)
        engine_oil_map[eng] = list(set([
            normalize_oil(f"{b}W-{g}") for b, g in oils
        ]))

    return engine_oil_map


def extract_engines(text):
    """
    Detect engine sizes from PDF text using two strategies.
    
    Step-by-step process:
    1. Split text into sentences
    2. Search each sentence for engine size pattern (1.4L, 2.4L, etc.)
    3. Validate size is between 0.8 and 8.0 liters
    4. Fallback: If no engines found, search for engine codes in mapping
    5. Return deduplicated list
    
    Args:
        text: Full PDF text
    
    Returns:
        List of unique engine sizes (e.g., ["1.4L", "2.4L"])
    """
    engines = []
    sentences = re.split(r'(?<!\d)[.!?](?!\d)|\n', text.lower())

    for sentence in sentences:
        matches = re.findall(ENGINE_PATTERN, sentence)
        for m in matches:
            try:
                num = float(m[0].strip())
                if 0.8 <= num <= 8.0:
                    engines.append(f"{num:.1f}L")
            except ValueError:
                continue

    if not engines:
        text_lower = text.lower()
        for code, displacement in ENGINE_CODE_MAP.items():
            if re.search(rf'\b{re.escape(code)}[a-z0-9]*\b', text_lower):
                engines.append(f"{displacement:.1f}L")

    return list(set(engines))


def has_engine_context(text, match_pos, match_text, context_window=150):
    """
    Validate engine type match appears in valid engine context.
    Prevents false positives from page numbers, codes, etc.
    
    Step-by-step process:
    1. Define engine-related keywords to check for
    2. Extract text window around match position
    3. For F8 (rare engine), require strict validation with "engine"/"type"/"spec"
    4. For other types, check for any engine keyword in context
    5. Return True if valid context found, False otherwise
    
    Args:
        text: Full PDF text (lowercase)
        match_pos: Character position of match start
        match_text: The matched text string
        context_window: Number of chars to check before/after match
    
    Returns:
        Boolean indicating if match has valid context
    """
    engine_keywords = [
        "engine", "displacement", "cc", "cylinder", "oil", "turbo",
        "configuration", "specs", "specifications", "performance",
        "l ", " l", "liter", "litre", "capacity", "horsepower", "hp"
    ]
    
    start = max(0, match_pos - context_window)
    end = min(len(text), match_pos + len(match_text) + context_window)
    context = text[start:end].lower()
    
    if match_text.lower() == "f8":
        close_context = text[max(0, match_pos - 50):min(len(text), match_pos + len(match_text) + 50)].lower()
        strict_keywords = ["engine", "type", "spec", "f8"]
        requires_engine = any(kw in close_context for kw in ["engine", "engine type", "engine configuration"])
        if not requires_engine:
            return False
    
    has_context = any(keyword in context for keyword in engine_keywords)
    return has_context


def extract_engine_types(text):
    """
    Identify engine types (V6, I4, TURBO, BOXER, etc.) from text.
    Validates matches against engine context to avoid false positives.
    
    Step-by-step process:
    1. Find all engine type pattern matches with character positions
    2. For each match, validate context with has_engine_context()
    3. Normalize text (uppercase, remove spaces)
    4. Handle aliases (BOXE→BOXER, FLAT→FLAT-6)
    5. Return deduplicated list
    
    Args:
        text: Full PDF text
    
    Returns:
        List of unique engine types found
    """
    engine_types = []
    text_lower = text.lower()
    
    for match in re.finditer(ENGINE_TYPE_PATTERN, text_lower):
        match_text = match.group()
        match_pos = match.start()
        
        if not has_engine_context(text_lower, match_pos, match_text):
            continue
        
        engine_type = match_text.strip().replace(" ", "").upper()
        
        if engine_type in ("BOXE", "BOXER"):
            engine_type = "BOXER"
        elif engine_type.startswith("FLAT"):
            engine_type = "FLAT-6"
        elif engine_type == "TWINTTURBO":
            engine_type = "TWIN-TURBO"
        
        engine_types.append(engine_type)
    
    return list(set(engine_types))


def engine_matches_capacity(engine, capacity):
    """
    Validate that engine size matches expected oil capacity.
    Typical small engines use 3-4 qts, medium 4-5.5, large 5+ qts.
    
    Args:
        engine: Engine size string (e.g., "1.4L")
        capacity: Capacity dict with "quarts" key
    
    Returns:
        Boolean indicating if engine/capacity pair makes sense
    """
    if not capacity:
        return True
    
    quarts = capacity.get("quarts", 0)
    size = float(engine.replace("L", ""))
    
    if size <= 1.6 and 3 <= quarts <= 4:
        return True
    if 1.7 <= size <= 2.5 and 4 <= quarts <= 5.5:
        return True
    if size >= 2.6 and quarts >= 5:
        return True
    
    return False


def extract_temperature(sentence):
    """
    Extract temperature values and conditions from sentence.
    Converts Celsius to Fahrenheit and classifies weather conditions.
    
    Step-by-step process:
    1. Find all temperature mentions (number + °C or °F)
    2. Convert Celsius values to Fahrenheit using (C × 9/5) + 32
    3. Add individual temperatures to result
    4. If multiple temps exist, add range
    5. Classify weather: below freezing (≤32°F), cold (<40°F), etc.
    6. Return set with temps, ranges, and classifications
    
    Args:
        sentence: Text to search for temperature data
    
    Returns:
        Set of temperature information strings
    """
    s = sentence.lower()
    temps = re.findall(TEMP_PATTERN, s)
    values = []
    
    for value, unit in temps:
        value = int(value)
        if unit == "c":
            value = round((value * 9 / 5) + 32)
        values.append(value)
    
    result = set()
    
    if not values:
        return {"all temperatures"}
    
    min_temp, max_temp = min(values), max(values)
    
    for temp in values:
        result.add(f"{temp}F")
    
    if len(values) > 1:
        result.add(f"range: {min_temp}F to {max_temp}F")
    
    if "below" in s and max_temp <= 32:
        result.add("below freezing")
    elif max_temp <= 40:
        result.add("cold weather")
    elif min_temp >= 85:
        result.add("hot weather")
    elif min_temp <= 0 and max_temp >= 100:
        result.add("all temperatures")
    elif 40 < max_temp < 85:
        result.add("moderate climate")
    elif "below" in s:
        result.add("cold weather")
    elif "above" in s:
        result.add("hot weather")
    else:
        result.add("all temperatures")
    
    return result


def extract_engine_capacities(doc):
    """
    Extract oil capacities for each engine from PDF document.
    Searches pages with oil specification keywords.
    
    Step-by-step process:
    1. Iterate through PDF pages
    2. Check page contains oil-related keywords or technical specs
    3. Find all engine sizes mentioned in order
    4. Extract all capacity values on page (numerically valid range)
    5. Attempt direct table mapping: engine_i ↔ capacity_i
    6. Fallback: Search text window around each engine for nearby capacity
    7. Store with_filter and without_filter variants (if found)
    
    Args:
        doc: PyMuPDF document object
    
    Returns:
        Dict mapping engine sizes to capacity objects
    """
    engine_caps = {}

    for page in doc:
        text = page.get_text().lower()

        if not any(kw in text for kw in OIL_PAGE_KEYWORDS):
            if "technical" not in text and "specification" not in text:
                continue

        seen_eng, engines_ordered = set(), []
        for m in re.finditer(ENGINE_PATTERN, text):
            key = f"{float(m.group(1)):.1f}L"
            if key not in seen_eng and 0.8 <= float(m.group(1)) <= 8.0:
                seen_eng.add(key)
                engines_ordered.append((key, m.start()))

        if not engines_ordered:
            continue

        all_caps = []
        for m in re.finditer(CAPACITY_PATTERN, text):
            q, l = to_quarts_liters(m.group(1), m.group(2))
            if 3.0 <= q <= 9.0:
                all_caps.append({"quarts": q, "liters": l, "pos": m.start()})

        if not all_caps:
            continue

        paired_caps = pair_quarts_liters(all_caps)

        if paired_caps and engines_ordered:
            for i in range(min(len(engines_ordered), len(paired_caps))):
                eng = engines_ordered[i][0]
                cap = paired_caps[i]
                engine_caps[eng] = {
                    "with_filter": cap,
                    "without_filter": None
                }
            continue

        for eng, eng_pos in engines_ordered:
            if eng in engine_caps and engine_caps[eng].get("with_filter"):
                continue

            window = text[max(0, eng_pos - 200): eng_pos + 400]

            if any(bad in window for bad in NON_ENGINE_CONTEXT):
                continue

            with_filter = None

            for m in re.finditer(CAPACITY_PATTERN, window):
                q, l = to_quarts_liters(m.group(1), m.group(2))
                if 3.0 <= q <= 9.0:
                    with_filter = {"quarts": q, "liters": l}
                    break

            if with_filter:
                engine_caps[eng] = {
                    "with_filter": with_filter,
                    "without_filter": None
                }

    return engine_caps


def extract_fallback_capacity(doc):
    """
    Extract generic oil capacity as fallback when engine-specific data unavailable.
    Used for documents with general oil info but no engine-specific specs.
    
    Step-by-step process:
    1. Search pages with oil keywords
    2. Priority: Look for "with filter" or "including filter" mentions
    3. Capture capacity value following filter keywords
    4. Fallback: Use first valid capacity found on page
    5. Return with_filter variant
    
    Args:
        doc: PyMuPDF document object
    
    Returns:
        Capacity dict or None if not found
    """
    for page in doc:
        text = page.get_text().lower()
        if not any(kw in text for kw in OIL_PAGE_KEYWORDS):
            continue
        
        wf_m = re.search(
            r"(?:with|including)\s*(?:filter|oil\s*filter)[^0-9]{0,60}" + CAPACITY_PATTERN,
            text
        )
        if wf_m:
            q, l = to_quarts_liters(wf_m.group(1), wf_m.group(2))
            if 3.0 <= q <= 9.0:
                return {"with_filter": {"quarts": q, "liters": l}, "without_filter": None}
        
        for m in re.finditer(CAPACITY_PATTERN, text):
            q, l = to_quarts_liters(m.group(1), m.group(2))
            if 3.0 <= q <= 9.0:
                return {"with_filter": {"quarts": q, "liters": l}, "without_filter": None}
    
    return None

def extract_oils(text):
    """
    Extract oil types with recommendation strength and temperature ranges.
    Implements multi-level priority hierarchy for oil temperature assignment.
    
    Step-by-step process:
    1. Split document into sentences for isolated context analysis
    2. Skip non-engine fluid contexts (transmission, brake, clutch, etc)
    3. Find oil specifications in each sentence and score by recommendation strength:
       - "preferred" or "recommended": +5 points
       - "may use" or "can use": +3 points
       - Other contexts: +1 point
    4. Extract temperature context using priority hierarchy:
       - PRIORITY 1: Keywords (never goes below, year-round) [highest]
       - PRIORITY 2: Actual temperature values extracted from sentence
       - PRIORITY 3: Descriptors (below, above) without actual temps
       - PRIORITY 4: Default ("all temperatures") [lowest]
    5. Scan entire document for uncoded oils (low score fallback)
    6. Apply document-level temperatures to oils with unknown temps only
    
    Args:
        text: Full PDF document text
    
    Returns:
        Tuple of (oil_scores dict, oil_temps dict)
        - oil_scores: oil type string → recommendation strength (int)
        - oil_temps: oil type string → temperature conditions set
    """
    
    sentences = re.split(r'(?<=[.!?])\s+', text)
    oil_scores = {}
    oil_temps = {}
    DEBUG_FILE = filename if 'filename' in globals() else ""

    for sent_idx, sentence in enumerate(sentences):
        lower = sentence.lower()

        if any(x in lower for x in NON_ENGINE_CONTEXT):
            continue

        oils = re.findall(OIL_PATTERN, sentence)
        if not oils:
            continue

        if DEBUG_FILE == "Copy of manual3.pdf":
            print(f"  Sentence {sent_idx}: {oils} -> {sentence[:80]}...")

        temps = set()
        
        if "never goes below" in lower:
            temps.add("above 20°F (-7°C)")
        elif "year-round" in lower or "all temperatures" in lower:
            temps.add("all temperatures")
        elif "below" in lower:
            extracted_temps = extract_temperature(sentence)
            has_specific_temps = any(
                "F" in t or "°" in t or "range:" in t 
                for t in extracted_temps 
                if t != "all temperatures"
            )
            if has_specific_temps:
                temps = extracted_temps
            else:
                temps.add("cold weather")
        elif "above" in lower:
            extracted_temps = extract_temperature(sentence)
            has_specific_temps = any(
                "F" in t or "°" in t or "range:" in t 
                for t in extracted_temps 
                if t != "all temperatures"
            )
            if has_specific_temps:
                temps = extracted_temps
            else:
                temps.add("hot weather")
        else:
            temps = extract_temperature(sentence)

        for base, grade in oils:
            oil = normalize_oil(f"{base}W-{grade}")

            if oil not in oil_scores:
                oil_scores[oil] = 0
                oil_temps[oil] = set()

            if "preferred" in lower or "recommended" in lower:
                oil_scores[oil] += 5
            elif "may use" in lower or "can use" in lower:
                oil_scores[oil] += 3
            else:
                oil_scores[oil] += 1

            if temps:
                oil_temps[oil].update(temps)
            elif not oil_temps[oil]:
                oil_temps[oil].add("all temperatures")

    all_oils = re.findall(OIL_PATTERN, text)

    for base, grade in all_oils:
        oil = normalize_oil(f"{base}W-{grade}")

        if oil not in oil_scores:
            pattern = f"{base}\\s*W[-–—]?\\s*{grade}"
            for match in re.finditer(pattern, text):
                start_pos = max(0, match.start() - 200)
                end_pos = min(len(text), match.end() + 200)
                context = text[start_pos:end_pos].lower()
                
                if not any(x in context for x in NON_ENGINE_CONTEXT):
                    oil_scores[oil] = 1
                    oil_temps[oil] = {"unknown"}
                    break

    doc_temps = extract_temperature(text)
    has_doc_specific_temps = any(
        "F" in t or "°" in t or "range:" in t 
        for t in doc_temps 
        if t != "all temperatures"
    )
    
    if has_doc_specific_temps:
        for oil in oil_scores:
            if "unknown" in oil_temps.get(oil, set()):
                oil_temps[oil] = doc_temps
    elif "never goes below" in text.lower():
        for oil in oil_scores:
            if "unknown" in oil_temps.get(oil, set()):
                oil_temps[oil] = {"above 20°F (-7°C)"}
    elif "year-round" in text.lower():
        for oil in oil_scores:
            if "unknown" in oil_temps.get(oil, set()):
                oil_temps[oil] = {"all temperatures"}

    return oil_scores, oil_temps

def select_best_engine(engine_caps, all_engines):
    """
    Select the most likely primary engine from candidates.
    
    Step-by-step process:
    1. If engine capacities available: iterate through engine_caps dict
    2. Validate engine matches its capacity expectations
    3. Return first engine with valid capacity match
    4. Fallback: Return first engine from engine_caps with its capacity
    5. If no capacities: rank engines by frequency (most common first)
    6. Filter realistic engines (1.0L to 3.5L range, excluding outliers)
    7. Return most common realistic engine or fallback to most frequent
    
    Args:
        engine_caps: Dict mapping engine sizes to capacity objects
        all_engines: List of all engines found in document
    
    Returns:
        Tuple of (engine_string, capacity_dict) or (None, None)
    """
    if engine_caps:
        for eng, cap in engine_caps.items():
            wf = cap.get("with_filter")
            if wf and engine_matches_capacity(eng, wf):
                return eng, cap
        first_key = next(iter(engine_caps))
        return first_key, engine_caps[first_key]

    if all_engines:
        engine_counts = {}
        for e in all_engines:
            engine_counts[e] = engine_counts.get(e, 0) + 1
        sorted_engines = sorted(engine_counts, key=engine_counts.get, reverse=True)
        realistic = [e for e in sorted_engines if 1.0 <= float(e.replace("L", "")) <= 3.5]
        best = realistic[0] if realistic else sorted_engines[0]
        return best, None

    return None, None

def extract_all():
    """
    Main orchestrator function for complete PDF extraction pipeline.
    Manages end-to-end data flow from Google Drive to JSON output file.
    
    Step-by-step process:
    1. Authenticate with Google Drive using service account credentials
    2. Discover all PDF files in target FOLDER_ID recursively
    3. For each PDF file:
       a. Extract vehicle info from filename (year-make-model.pdf)
       b. Download PDF from Google Drive
       c. Parse PDF pages and extract full text
       d. Detect vehicle details from PDF content (fallback)
    4. Extract all technical specifications from PDF text:
       - Engine capacities (per-engine if available)
       - Oil types with recommendation scores
       - Oil temperature conditions
       - Engine types (V6, I4, TURBO, etc)
       - Engine-oil proximity mapping
    5. Build per-engine oil recommendation data:
       - Select best engine from candidates
       - Map oils to specific engines via proximity scoring
       - Build recommendation structure per engine
    6. Fallback handling: If no engine data found:
       - Extract generic PDF-level capacity
       - Use all oils with generic/unknown engine label
       - Map document-level temperatures to all oils
    7. Filter and prioritize oil recommendations:
       - Primary oil: highest recommendation score
       - Secondary oils: within -2 points of primary
       - Include any oil with specific temperature conditions
    8. Construct result record with vehicle metadata and engine oil data
    9. Write all vehicle records to JSON output file (structured_results.json)
    10. Display completion message
    
    Args:
        None (uses global constants: FOLDER_ID, OUTPUT_FILE)
    
    Returns:
        None (writes to OUTPUT_FILE)
    """
    service = get_drive_service()
    pdfs = get_all_pdfs(service, FOLDER_ID)
    print(f"\nTotal PDFs found: {len(pdfs)}\n")
    results = {}

    for file in pdfs:
        filename = file["name"]
        print("Processing:", filename)

        year, make, model = parse_filename(filename)
        pdf_stream = download_pdf(service, file["id"])

        with fitz.open(stream=pdf_stream.read(), filetype="pdf") as doc:
            if year is None:
                y2, m2, mo2 = detect_vehicle_from_pdf(doc)
                year  = year  or y2
                make  = make  or m2
                model = model or mo2

            text        = clean_text(" ".join(p.get_text("text") for p in doc))
            engine_caps = extract_engine_capacities(doc)
            oil_scores, oil_temps = extract_oils(text)
            
            for oil in oil_temps:
                temps = oil_temps[oil]
                specific_temps = {t for t in temps if "°" in t or "F" in t or "range:" in t or "above" in t or "below" in t}
                if specific_temps and "all temperatures" in temps:
                    oil_temps[oil] = specific_temps
            all_engines = extract_engines(text)
            engine_types = extract_engine_types(text)
            engine_oil_map = map_oils_to_engines(text)

            if not engine_caps and all_engines:
                caps = []

                for m in re.finditer(CAPACITY_PATTERN, text):
                    q, l = to_quarts_liters(m.group(1), m.group(2))
                    if 3.0 <= q <= 9.0:
                        caps.append({"quarts": q, "liters": l})

                paired_caps = pair_quarts_liters(caps)

                for i in range(min(len(all_engines), len(paired_caps))):
                    engine_caps[all_engines[i]] = {
                        "with_filter": paired_caps[i],
                        "without_filter": None
                    }

            selected_engine, selected_cap_entry = select_best_engine(engine_caps, all_engines)
            multi_engine_data = build_multi_engine_data(engine_caps, oil_scores, oil_temps, engine_oil_map)
            
            if not multi_engine_data:
                fallback_cap = extract_fallback_capacity(doc)

                oil_list = []
                if oil_scores:
                    primary = max(oil_scores, key=oil_scores.get)
                    max_score = oil_scores[primary]
                    
                    for oil, score in oil_scores.items():
                        temps = oil_temps.get(oil, [])
                        has_actual_temps = any(
                            ("°F" in t or "°C" in t or "F" in t or "C" in t or 
                             "above" in t or "below" in t or "range:" in t or 
                             "weather" in t or "temperatures" in t or "cold" in t or "hot" in t)
                            for t in temps
                        )
                        
                        if score >= max_score - 2 or has_actual_temps:
                            oil_list.append({
                                "oil_type": oil,
                                "recommendation_level": "primary" if oil == primary else "secondary",
                                "temperature_condition": list(temps)
                            })

                if fallback_cap:
                    multi_engine_data = {
                        "unknown_engine": {
                            "oil_capacity": fallback_cap,
                            "oil_recommendations": oil_list
                        }
                    }
            
            with_filter_cap    = selected_cap_entry.get("with_filter")    if selected_cap_entry else None
            without_filter_cap = selected_cap_entry.get("without_filter") if selected_cap_entry else None

            oil_list = []
            if oil_scores:
                primary   = max(oil_scores, key=oil_scores.get)
                max_score = oil_scores[primary]
                for oil, score in oil_scores.items():
                    if score >= max_score - 2 or any(
                        "weather" in t or "temperatures" in t
                        for t in oil_temps[oil]
                    ):
                        oil_list.append({
                            "oil_type": oil,
                            "recommendation_level": "primary" if oil == primary else "secondary",
                            "temperature_condition": list(oil_temps[oil]),
                        })
            
            results[filename] = {
                "Vehicle": {
                    "year": year,
                    "make": make,
                    "model": model,
                    "engine_types": list(engine_types),
                    "displayName": f"{year} {make} {model}"
                },
                "engines": multi_engine_data
            }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)

    print("\nExtraction Complete\n")


if __name__ == "__main__":
    extract_all()