UPDATE txm_bitx.provider bp
JOIN txm_bitx.bill_header b ON bp.bill_id = b.bill_id
SET bp.rendering_provider_nabp = bp.rendering_provider_license,
bp.rendering_provider_license = NULL
WHERE b.source = 'ebill'
AND b.form_type = 'RX'
AND bp.rendering_provider_license IS NOT NULL;