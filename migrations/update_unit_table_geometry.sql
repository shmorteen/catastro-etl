UPDATE catastro_units u
SET geometry = p.geometry
FROM catastro_parcels p
WHERE u.parcel_ref = p.referencia_catastral
  AND u.geometry IS NULL;
