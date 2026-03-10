from app.services.scoring import compute_zefix_score, compute_zefix_score_breakdown


def test_being_cancelled_forces_zero_score():
    score = compute_zefix_score(
        legal_form="GmbH",
        legal_form_short_name="gmbh",
        capital_nominal="100000",
        purpose="Beratung und Softwareentwicklung",
        branch_offices="[]",
        industry="Consulting",
        status="being_cancelled",
        canton="BE",
        municipality="Bern",
    )
    assert score == 0


def test_consulting_industry_applies_penalty():
    base = compute_zefix_score(
        legal_form="GmbH",
        legal_form_short_name="gmbh",
        capital_nominal="100000",
        purpose="Beratung und Dienstleistungen fuer Unternehmen",
        branch_offices="[]",
        industry="Technology",
        status="aktiv",
        canton="BE",
        municipality="Bern",
    )
    consulting = compute_zefix_score(
        legal_form="GmbH",
        legal_form_short_name="gmbh",
        capital_nominal="100000",
        purpose="Beratung und Dienstleistungen fuer Unternehmen",
        branch_offices="[]",
        industry="Consulting",
        status="aktiv",
        canton="BE",
        municipality="Bern",
    )
    assert consulting == max(0, base - 15)


def test_treuhand_industry_applies_penalty():
    base = compute_zefix_score(
        legal_form="AG",
        legal_form_short_name="ag",
        capital_nominal="500000",
        purpose="Finanzverwaltung und Beratung",
        branch_offices="[]",
        industry="Finance",
        status="active",
        canton="BE",
        municipality="Bern",
    )
    treuhand = compute_zefix_score(
        legal_form="AG",
        legal_form_short_name="ag",
        capital_nominal="500000",
        purpose="Finanzverwaltung und Beratung",
        branch_offices="[]",
        industry="Treuhand Services",
        status="active",
        canton="BE",
        municipality="Bern",
    )
    assert treuhand == max(0, base - 15)


def test_score_breakdown_contains_final_score():
    breakdown = compute_zefix_score_breakdown(
        legal_form="GmbH",
        legal_form_short_name="gmbh",
        capital_nominal="250000",
        purpose="Entwicklung von Softwareloesungen fuer Unternehmen",
        branch_offices="[]",
        industry="Technology",
        status="aktiv",
        canton="BE",
        municipality="Bern",
    )
    assert "final_score" in breakdown
    assert isinstance(breakdown["final_score"], int)
