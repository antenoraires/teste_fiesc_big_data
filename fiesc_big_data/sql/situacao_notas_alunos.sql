SELECT  M.id_matricula,
        M.id_disciplina,
    -- D.nome_disciplina,
        D.nome_curso,
        AVG(M.nota) as nota,
        case when AVG(M.nota) >= 7 then 'Aprovado'
        else 'Reprovado'
        end as situacao
        FROM matricula_notas M
        Left join (select D.nome_disciplina,
                D.id_disciplina,
                C.nome_curso from disciplinas D
        left join cursos C on D.id_curso = C.id_curso) D
        on M.id_disciplina = D.id_disciplina
        group by M.id_matricula, 
        M.id_disciplina, 
        --D.nome_disciplina,
        D.nome_curso