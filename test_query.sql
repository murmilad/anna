SELECT count(*), inn  
	FROM faces
GROUP BY inn

SELECT inn, owner, species, pledge, apple_date_from, date_ins
	FROM faces
WHERE inn IN ('515482722', '234876877', '364121353')
ORDER BY inn, date_ins

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

SELECT  *, 
				LAG(owner,1) OVER(ORDER BY inn, date_ins) prev_owner,
				LAG(species,1) OVER(ORDER BY inn, date_ins) prev_species,
				CASE WHEN prev_inn IS null OR inn != prev_inn THEN 1 ELSE 0 END AS start_inn
				FROM (
					SELECT *,
						LAG(inn, 1) OVER(ORDER BY inn, date_ins) prev_inn,
						LEAD(inn, 1) OVER(ORDER BY inn, date_ins) next_inn
					FROM faces
					WHERE inn IN ('515482722', '234876877', '364121353') 
					ORDER BY inn, date_ins
				) ordered_faces
			ORDER BY inn, date_ins
		
SELECT inn, owner, species, pledge, apple_date_from, date_ins,
	CASE 
		WHEN is_first_in_inn = 1 THEN apple_date_from
		ELSE date_ins END 
	AS date_from,
	TO_CHAR (CASE 
		WHEN is_last_in_inn = 1 THEN '2999-12-31'
		ELSE next_date_ins - interval '1 day' END 
	, 'yyyy-MM-DD ') AS date_to
	FROM (
		SELECT *,
			CASE WHEN prev_inn IS null OR inn != prev_inn THEN 1 ELSE 0 END AS is_first_in_inn,
			CASE WHEN next_inn IS null OR inn != next_inn THEN 1 ELSE 0 END AS is_last_in_inn
			FROM (
				SELECT *,
					LEAD(date_ins,1) OVER(ORDER BY inn, date_ins) next_date_ins,
					LAG(inn, 1) OVER(ORDER BY inn, date_ins) prev_inn,
					LEAD(inn, 1) OVER(ORDER BY inn, date_ins) next_inn
					FROM (
						SELECT  *, 
							LAG(owner,1) OVER(ORDER BY inn, date_ins) prev_owner,
							LAG(species,1) OVER(ORDER BY inn, date_ins) prev_species,
							CASE WHEN prev_start_inn IS null OR inn != prev_start_inn THEN 1 ELSE 0 END AS start_inn
							FROM (
								SELECT *,
									LAG(inn, 1) OVER(ORDER BY inn, date_ins) prev_start_inn
								FROM faces
								WHERE inn IN ('515482722', '234876877', '364121353') 
								ORDER BY inn, date_ins
							) ordered_faces
						ORDER BY inn, date_ins
					) AS param_faces
				WHERE 
					start_inn = 1 
					OR coalesce(owner, '!null!') != coalesce(prev_owner, '!null!')
					OR coalesce(species, '!null!') != coalesce(prev_species, '!null!')
				ORDER BY inn, date_ins
				) AS where_faces
		) AS inn_faces
ORDER BY inn, date_ins