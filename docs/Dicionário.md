# Contextualização

Conjunto de dados educacionais gerados de forma aleatória e com dados fictícios.

## Dicionário de dados

### alunos

Dados dos alunos. Esta tabela contém informações pessoais sobre os alunos.

|Campo | Descrição |
|:--------:|------:|
| id | Identificador único do registro de pessoa do aluno |
| nome | Nome do aluno |
| data_nascimento | Data de nascimento do aluno |

### cursos

Dados dos cursos. Esta tabela contém informações sobre os diferentes cursos oferecidos.

| Campo | Descrição |
|:--------:|------:|
| CÓDIGO | Identificador único |
| NOME | Nome do curso |
| ÁREA DE ATUAÇÃO | Área de atuação do curso |

### disciplinas

Dados das disciplinas. Esta tabela contém informações sobre as diferentes disciplinas oferecidas nos cursos.

| Campo | Descrição |
|:--------:|------:|
| CÓDIGO DISC | Identificador único |
| CÓDIGO CURSO | Código do curso |
| NOME | Nome da disciplina |
| SIGLA | Sigla da disciplina |
| DIA DA SEMANA | Dia da semana que acontecem as aulas da disciplina (SEGUNDA = 0 ... DOMINGO = 6) |
| CARGA HORÁRIA | Carga horária da disciplina |
| AVALIAÇÕES | Quantidade de avaliações da disciplina |

### turmas

Dados das turmas. Esta tabela contém informações sobre as diferentes turmas formadas para cada curso.

| Campo | Descrição |
|:--------:|------:|
| CÓDIGO | Identificador único |
| CÓDIGO CURSO | Código do curso da turma |
| NOME | Nome da turma |
| DATA DE INÍCIO | Data de início da turma |
| DATA DE TÉRMINO | Data de término da turma |

### turma_avaliacoes

Dados das avaliações das turmas. Esta tabela contém informações sobre as diferentes avaliações realizadas em cada turma.

| Campo | Descrição |
|:--------:|------:|
| titulo | Título da avaliação |
| id_turma | Código da turma |
| id_disciplina | Código da disciplina |
| data_avaliacao | Data de aplicação da avaliação |

### matriculas

Dados das matrículas. Esta tabela contém informações sobre as matrículas dos alunos nas diferentes turmas.

| Campo | Descrição |
|:--------:|------:|
| id_matricula | Identificador único da Matrícula / Registro acadêmico |
| id_turma | Código da turma |
| id_aluno | Código do aluno (pessoa) |
| data_matricula | Data de realização da matrícula |
| situacao | Situação da matrícula |
| data_situacao | Data de atualização da situação da matrícula |

### matricula_disciplinas

Dados das disciplinas cursadas nas matrículas. Esta tabela contém informações sobre as disciplinas que cada aluno está matriculado.

| Campo | Descrição |
|:--------:|------:|
| id_matricula | Código da matrícula |
| id_disciplina | Código da disciplina |

### matricula_aulas

Dados das presenças nas aulas. Esta tabela contém informações sobre a presença dos alunos nas aulas.

| Campo | Descrição |
|:--------:|------:|
| id_matricula | Código da matrícula |
| id_disciplina | Código da disciplina |
| data_aula | Data de realização da aula |
| presente | Flag de presença do aluno |

### matricula_notas

Dados das notas dos alunos. Esta tabela contém informações sobre as notas que os alunos receberam em suas avaliações.

| Campo | Descrição |
|:--------:|------:|
| id_matricula | Código da matrícula |
| id_disciplina | Código da disciplina |
| data | Data que a nota foi aplicada |
| nota | Nota do aluno na avaliação |
