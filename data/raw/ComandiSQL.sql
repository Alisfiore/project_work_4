-- Visualizzare il numero di corsi in cui insegna ciascun professore
SELECT p.nome, p.cognome, c.corso, COUNT(c.Pk_id_corso) AS numero_corsi
FROM professori p
JOIN corsi c ON p.dipartimento = c.area
GROUP BY p.nome, p.cognome, c.corso
ORDER BY p.nome, p.cognome;


-- Visualizzare i dipartimenti in cui insegnano professori il cui cognome inizia per C
SELECT DISTINCT p.dipartimento
from professori p
WHERE p.cognome LIKE 'C%';


-- Visualizzare i corsi di Ennio Marangoni
SELECT c.corso, p.nome, p.cognome
FROM Professori p
JOIN Corsi c ON p.dipartimento = c.area
WHERE p.nome = 'Ennio' AND p.cognome = 'Marangoni';


-- Visualizzare gli studenti che hanno corsi con Cassandra Querini o Maria Raimondi
SELECT s.nome, s.cognome
FROM studenti s
JOIN iscrizioni i ON s.Id_studente = i.Id_studente
JOIN corsi c ON i.Id_corso = c.Pk_id_corso
JOIN professori p ON c.Area = p.Dipartimento
WHERE (p.nome = 'Cassandra' AND p.cognome = 'Querini')
   OR (p.nome = 'Maria' AND p.cognome = 'Raimondi');

-- Visualizzare tutti i professori delle sorelle Luigia e Rosa Trezzini

-- Visualizzare il numero di studenti iscritti in ciascun dipartimento

-- Visualizzare il numero di studenti iscritti a ciascun corso

-- Visualizzare il numero di studenti di Pierpaolo Mascagni

-- Visualizzare gli studenti iscritti all'anno accademico 2023-2024

-- Visualizzare i corsi a cui partecipano gli studenti scritti all'anno accademico 2023-2024