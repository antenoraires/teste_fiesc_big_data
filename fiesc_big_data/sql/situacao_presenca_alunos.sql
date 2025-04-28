select
M.id_matricula,
M.id_disciplina,
D.nome_curso,
avg(cast(M.presente as int)) as presenca,
case 
when avg(cast(M.presente as int)) >= 0.7 then 'Aprovado'
else 'Reprovado' end as situacao
from matricula_aulas M
Left join (select
                D.id_disciplina,
                C.nome_curso from disciplinas D
        left join cursos C on D.id_curso = C.id_curso) D
        on D.id_disciplina = M.id_disciplina
group by 
M.id_matricula,
M.id_disciplina,
D.nome_curso  