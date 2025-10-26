import pytest
from pathlib import Path
from box import Box
from charapi.data.charity_evaluation_result import (
    CharityEvaluationResult,
    FinancialMetrics,
    ComplianceCheck,
    ExternalValidation,
    OrganizationType
)

def get_included_fields():
    config_path = Path(__file__).parent.parent / "fidcsv" / "config" / "config.yaml"
    config = Box.from_yaml(filename=config_path)
    return [
        field_name
        for field_name, field_config in config.fields.items()
        if field_config.include
    ]

def test_included_fields_not_empty():
    fields = get_included_fields()
    assert len(fields) > 0

def test_field_extraction_from_result():
    financial_metrics = FinancialMetrics(
        program_expense_ratio=0.75,
        admin_expense_ratio=0.15,
        fundraising_expense_ratio=0.10,
        net_assets=1000000,
        total_revenue=500000,
        total_expenses=450000,
        program_expenses=337500,
        admin_expenses=67500,
        fundraising_expenses=45000,
        total_assets=1500000,
        total_liabilities=500000
    )

    compliance = ComplianceCheck(
        is_compliant=True,
        issues=[],
        in_pub78=True,
        is_revoked=False,
        has_recent_filing=True
    )

    external = ExternalValidation(
        charity_navigator_rating=4,
        charity_navigator_score=95.0,
        has_transparency_seal=True,
        has_advisory_alerts=False,
        negative_news_alerts=0
    )

    org_type = OrganizationType(
        score=100.0,
        issues=[],
        subsection=3,
        foundation_type=15,
        filing_requirement=1,
        years_operating=10
    )

    result = CharityEvaluationResult(
        ein="12-3456789",
        organization_name="Test Charity",
        score=85.0,
        alignment_score=90,
        metrics=[],
        financial_metrics=financial_metrics,
        compliance_check=compliance,
        external_validation=external,
        organization_type=org_type,
        evaluation_timestamp="2025-10-25T12:00:00",
        data_sources_used=["ProPublica", "CharityAPI"],
        outstanding_count=5,
        acceptable_count=3,
        unacceptable_count=1,
        total_metrics=9,
        summary="Test summary"
    )

    assert hasattr(result, "ein")
    assert result.ein == "12-3456789"
    assert hasattr(result.financial_metrics, "program_expense_ratio")
    assert result.financial_metrics.program_expense_ratio == 0.75
    assert hasattr(result.compliance_check, "in_pub78")
    assert result.compliance_check.in_pub78 is True
