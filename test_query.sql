SELECT count(*), inn  
	FROM faces
GROUP BY inn

SELECT inn, owner, species, pledge, sex, birth, death, apple_date_from, date_ins
	FROM faces
WHERE inn = '515482722'
ORDER BY date_ins

insert into faces (inn, name, owner, species, pledge, sex, birth, death, apple_date_from, date_ins)
	values
		('515482722', 'Thor Anders', 'Johan Stig', 'AAA', 580754.239556708, 'M', '1981-01-06', NULL, '2018-04-24', '2020-03-22')

	

SELECT *
	FROM (
    	SELECT date_ins lag(date_ins,1) OVER w_faces as previous_date_ins, lead(date_ins,1) OVER w_faces as next_date_ins
    		FROM faces
    	WHERE inn = '515482722'
	    WINDOW w_faces AS (
	        PARTITION BY date_ins
	        ORDER BY date_ins
	    )
	)
) pn_faces

		SELECT *,
					ROW_NUMBER() OVER (
 						PARTITION BY inn
					    ORDER BY date_ins
					) AS rownum_asc,
					ROW_NUMBER() OVER (
 						PARTITION BY inn
					    ORDER BY date_ins DESC
					) AS rownum_desc
					FROM faces
		WHERE inn = '515482722' 


SELECT inn, owner, species, pledge, birth, apple_date_from, date_ins,
	CASE 
		WHEN rownum_asc = 1 THEN apple_date_from
		ELSE date_ins END 
	AS date_from,
	TO_CHAR (CASE 
		WHEN next_date_ins IS NULL THEN '2999-12-31'
		ELSE next_date_ins - interval '1 day' END 
	, 'yyyy-MM-DD ') AS date_to
	FROM (SELECT *,
		LEAD(date_ins,1) OVER(ORDER BY date_ins) next_date_ins
		FROM (
			SELECT  *, 
				LAG(owner,1) OVER(ORDER BY date_ins) prev_owner,
				LAG(species,1) OVER(ORDER BY date_ins) prev_species
				FROM (
					SELECT *,
						ROW_NUMBER() OVER (
	 						PARTITION BY inn
						    ORDER BY date_ins
						) AS rownum_asc,
						ROW_NUMBER() OVER (
	 						PARTITION BY inn
						    ORDER BY date_ins DESC
						) AS rownum_desc
					FROM faces
				) ordered_faces
			WHERE inn = '515482722' 
		) AS lagged_faces
	WHERE 
		rownum_asc = 1 
		OR (
			rownum_desc = 1 
			AND (
				owner != prev_owner  
				OR species != prev_species 
			)
		)
		OR owner != prev_owner  
		OR species != prev_species 
	) AS leaded_faces