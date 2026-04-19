import re
from datetime import datetime


def clean_review(review: dict) -> dict:
    content = review.get("content", "") or ""

    return {
        "review_id":      review.get("reviewId"),
        "content":        content.strip(),
        "score":          review.get("score"),
        "thumbs_up":      review.get("thumbsUpCount", 0),
        "app_version":    review.get("reviewCreatedVersion"),
        "reviewed_at":    str(review.get("at")),
        "reply_content":  review.get("replyContent"),
        "replied_at":     str(review.get("repliedAt")) if review.get("repliedAt") else None,
    }


def assign_sentiment(score: int) -> str:
    if score >= 4:
        return "positive"
    elif score == 3:
        return "neutral"
    else:
        return "negative"


def clean_text(text: str) -> str:
    """Lowercase and strip punctuation for keyword matching."""
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    return text