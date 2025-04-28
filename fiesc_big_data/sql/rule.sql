WITH 
nome_turmas AS (
    SELECT 
        S.*,
        T.nome_turma,
        T.data_inicio,
        T.data_fim,
        T.codigo_curso
    FROM 
        situacao_turmas S
    LEFT JOIN turmas T ON S.id_turma = T.id_turma
),

situacao AS (
    SELECT 
        S.id_matricula,
        S.id_disciplina,
        CASE 
            WHEN S.situacao = 'Aprovado' AND P.situacao = 'Aprovado' THEN 'Aprovado'
            ELSE 'Reprovado'
        END AS situacao_final
    FROM situacao_notas S
    LEFT JOIN situacao_presenca P ON (S.id_matricula = P.id_matricula AND
                                    S.id_disciplina = P.id_disciplina)
)

SELECT 
    S.id_matricula,
    T.id_turma,
    A.id AS id_aluno,
    T.data_inicio as data_matricula,       
    CASE 
        WHEN S.situacao_final = 'Aprovado' AND A.idade_valida = 'sim' THEN 'Aprovado'
        ELSE 'Reprovado'
    END AS siatuacao,
    T.data_fim as data_situacao
FROM situacao S
LEFT JOIN nome_turmas T ON (S.id_disciplina = T.id_disciplina)
LEFT JOIN alunos A ON S.id_matricula = A.id