from processing.cleaner import clean_text

CATEGORIES = {
    "verification": [
        # english
        "verify", "verification", "kyc", "kyc failed", "kyc pending",
        "national id", "valid id", "selfie", "face verification", "id card", "id", "passport", "scan", "won't scan",
        "identity", "id verification", "account verification", "driver's license", "residence certificate",
        "verified", "not verified", "verification failed", "tin number", "ssn", "social security number",
        "under review", "verification process",

        # tagalog / taglish
        "ma-verify", "na-verify", "i-verify", "ma verify",
        "hindi ma verify", "ayaw ma verify",
        "di ma verify", "di maverify",
        "pending verification", "for verification",
        "hindi ma approve", "di ma approve",
        "verification rejected", "rejected id", "hindi ma-scan",
    ],

    "transaction": [
        # english
        "transfer", "send money", "payment", "pay", "paid",
        "failed transaction", "transaction failed",
        "pending", "still pending",
        "cashout", "cash out", "cash in", "cashin",
        "transaction", "deducted", "double charge",
        "not received", "did not receive",
        "refund", "no refund", "refund not received",
        "processing", "delay", "delayed transaction",

        # tagalog / taglish
        "padala", "nagpadala", "bayad", "nagbayad",
        "hindi natanggap", "di natanggap",
        "na-deduct", "nabawas", "kinaltas",
        "nawala", "nawalan", "pera", "pambili",
        "hindi dumating", "di dumating",
        "walang pumasok", "di pumasok",
        "doble bayad", "double bayad",
        "wala pa refund", "di pa refund",
    ],

    "login": [
        # english
        "login", "log in", "cannot login", "cant login",
        "unable to login", "sign in", "sign-in",
        "otp", "otp not received", "no otp", "cannot access", "can't access gcash",
        "password", "wrong password", "cannot open", "can't open", "mpin", "pin not working",
        "locked out", "account locked", "can't install", "cannot install",
        "access denied", "authentication", "can't load", "cannot load",
        "session expired", "expired session", "Cant open", "cant open",

        # tagalog / taglish
        "di makapasok", "hindi makapasok",
        "di makalogin", "hindi makalogin",
        "di maka login", "hindi maka login",
        "naka-lock", "na lock account",
        "di matanggap otp", "walang otp",
        "di ako maka access", "di ma access",
    ],

    "performance": [
        # english
        "slow", "very slow", "lag", "laggy",
        "freeze", "freezing", "hang", "hanging",
        "crash", "crashing", "app crash",
        "loading", "stuck loading", "log",
        "error", "server error", "network error",
        "bug", "bugs", "glitch", "issue",
        "not working", "stopped working",
        "force close", "auto close", "loading",
        "blank screen", "white screen",
        "not opening", "won't open",

        # tagalog / taglish
        "mabagal", "sobrang bagal",
        "nag-crash", "nag crash",
        "nagha-hang", "nag hang", "nagha-hang", "nag hang",
        "hindi naglo-load", "di naglo load",
        "hindi nagbubukas", "di nagbubukas",
        "biglang nagsara", "kusang nagsara",
        "hindi gumagana", "di gumagana",
        "may error", "laging error",
        "puti screen", "white screen",
    ],

    "ux": [
        # english
        "confusing", "hard to use", "difficult to use",
        "ui", "ux", "interface", "design",
        "navigation", "complicated",
        "not intuitive", "unintuitive",
        "too many steps", "poor design",
        "bad layout", "cluttered",

        # tagalog / taglish
        "mahirap gamitin", "ang hirap gamitin",
        "magulo", "nakakalito",
        "hindi madaling gamitin",
        "di user friendly", "hindi user friendly",
        "pangit design", "pangit ui",
        "ang daming steps",
    ],

    "feature": [
        # english
        "feature", "new feature", "add feature",
        "suggestion", "request", "feature request",
        "wish", "please add", "would be nice",
        "improve", "improvement",
        "add option", "missing feature",

        # tagalog / taglish
        "sana", "sana meron",
        "idagdag", "magdagdag",
        "gusto ko", "gusto sana",
        "pwede bang", "pwede sana",
        "mas maganda kung",
        "kulang feature", "wala yung feature",
    ],

    "praise": [
        # english
        "love", "i love", "great", "excellent",
        "amazing", "best app", "awesome",
        "perfect", "fantastic", "good app",
        "very useful", "helpful", "superb", "nice", "wonderful",
        "highly recommend", "recommended",
        "easy to use", "convenient", "Good", "Secured",
        "smooth", "fast", "reliable", "easy", "easily", "useful", "well served", "good", "ok", 
"nice", "helpful", "convenient", "smooth", "fast", "reliable", "easy", "easily", "useful", "well served", "good", "ok",

        # tagalog / taglish
        "maganda", "magaling", "ayos",
        "salamat", "thank you",
        "napakaganda", "napakagaling",
        "sobrang ganda", "sobrang ayos",
        "ang galing", "ang ganda",
        "okay na okay", "solid", "madali",
    ],
}


def categorize_review(content: str) -> str:
    """
    Assign a single category to a review based on keyword matching.
    Returns the first matching category in priority order,
    or 'other' if no keywords match.
    """
    if not content:
        return "other"

    cleaned = clean_text(content)

    for category, keywords in CATEGORIES.items():
        for keyword in keywords:
            if keyword in cleaned:
                return category

    return "other"