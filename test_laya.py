#!/usr/bin/env python3
"""Test script for Laya - demonstrates Router and core features."""

from laya import Router


def test_english_routing():
    """Test English text routes to english checkpoint."""
    router = Router()
    
    state = {
        "from": "user@acme.com",
        "subject": "Duplicate charge on invoice #4411",
        "body": "Hi, we were billed twice for March. Please refund the duplicate today or we will cancel."
    }
    
    questions = {
        "department": {
            "type": "choice",
            "instructions": "Which department should handle this request?",
            "criteria": {
                "billing": "invoices, payments, refunds",
                "technical": "bugs, outages, system errors",
                "sales": "pricing, new contracts",
                "other": "everything else"
            }
        },
        "urgency": {
            "type": "score",
            "instructions": "How urgent is this request?",
            "criteria": ["not urgent", "soon", "critical deadline or blocking issue"]
        },
        "churn_risk": {
            "type": "noul",
            "instructions": "Does the user threaten to cancel or leave?"
        },
        "refund_requested": {
            "type": "noul",
            "instructions": "Does the user explicitly request a refund?"
        }
    }
    
    result = router.predict(state, questions)
    
    print("=== English Test ===")
    print(f"Department: {result['answers']['department']['choice']} (confidence: {result['answers']['department']['confidence']:.3f})")
    print(f"Urgency: {result['answers']['urgency']['score']:.2f} / {len(questions['urgency']['criteria']) - 1}")
    print(f"Churn risk: {result['answers']['churn_risk']['noul']:.3f}")
    print(f"Refund requested: {result['answers']['refund_requested']['noul']:.3f}")
    print(f"Routing: {result['routing']['model']}")
    print(f"Reason: {result['routing']['reason']}")
    print()


def test_multilingual_routing():
    """Test non-Latin script routes to multilingual checkpoint."""
    router = Router()
    
    state = {"body": "मुझसे दो बार शुल्क लिया गया, कृपया पैसे वापस करें।"}
    questions = {
        "department": {"type": "choice", "instructions": "Which department?", "criteria": {"billing": "refunds", "tech": "bugs"}},
        "churn_risk": {"type": "noul", "instructions": "Does the user threaten to cancel?"}
    }
    
    result = router.predict(state, questions)
    
    print("=== Hindi (Devanagari) Test ===")
    print(f"Department: {result['answers']['department']['choice']} (confidence: {result['answers']['department']['confidence']:.3f})")
    print(f"Churn risk: {result['answers']['churn_risk']['noul']:.3f}")
    print(f"Routing: {result['routing']['model']}")
    print(f"Reason: {result['routing']['reason']}")
    print()


def test_routing_inspection():
    """Test routing inspection without forward pass."""
    router = Router()
    
    print("=== Routing Inspection (no forward pass) ===")
    print(f"German: {router.route({'body': 'Der Kunde wurde zweimal belastet'}).reason}")
    print(f"Portuguese: {router.route({'body': 'Quero cancelar'}).reason}")
    print(f"English: {router.route({'body': 'Please refund the duplicate charge'}).reason}")
    print()


def test_batch_prediction():
    """Test multiple predictions for throughput measurement."""
    router = Router()
    
    states = [
        {"body": f"Request {i}: Customer wants refund for order #{1000+i}"}
        for i in range(10)
    ]
    
    questions = {
        "intent": {"type": "choice", "instructions": "What is the intent?", "criteria": {"refund": "wants money back", "inquiry": "asking question", "complaint": "unhappy"}}
    }
    
    import time
    start = time.time()
    results = [router.predict(s, questions) for s in states]
    elapsed = time.time() - start
    
    print("=== Sequential Predictions (10 requests) ===")
    for i, r in enumerate(results):
        print(f"  {i}: {r['answers']['intent']['choice']} ({r['answers']['intent']['confidence']:.3f})")
    print(f"Total time: {elapsed*1000:.1f} ms ({elapsed*100:.1f} ms/req)")
    print()


def test_confidence_gating():
    """Demonstrate automated confidence gating."""
    router = Router()
    
    state = {"body": "Hello, I have a question about my bill."}
    questions = {
        "topic": {"type": "choice", "instructions": "Topic?", "criteria": {"billing": "bill", "technical": "bug", "other": "else"}}
    }
    
    result = router.predict(state, questions)
    answer = result["answers"]["topic"]
    
    print("=== Confidence Gating ===")
    print(f"Choice: {answer['choice']}, Confidence: {answer['confidence']:.3f}")
    
    if answer["confidence"] >= 0.85:
        print("HIGH CONFIDENCE -> Automated action")
    else:
        print("LOW CONFIDENCE -> Escalate to human")
    print()


def test_workflow_presets():
    """Test built-in workflow presets."""
    import laya
    from laya import load
    
    agent = load("convaiinnovations/laya")
    
    print("=== Workflow Presets ===")
    
    # Triage
    triage = agent.predict({"message": "My payment failed twice and I'm furious"}, laya.triage_questions())
    print(f"Triage intent: {triage['answers']['intent']['choice']} (conf: {triage['answers']['intent']['confidence']:.3f})")
    print(f"Triage urgency: {triage['answers']['urgency']['score']:.2f}")
    print(f"Triage frustration: {triage['answers']['frustration']['score']:.2f}")
    print()


def main():
    test_english_routing()
    test_multilingual_routing()
    test_routing_inspection()
    test_batch_prediction()
    test_confidence_gating()
    test_workflow_presets()
    print("All tests completed!")


if __name__ == "__main__":
    main()