PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE [a] (
   title TEXT,
   ref TEXT
);
INSERT INTO a VALUES('Adromischus cristatus (Crinkle Leaf Plant)','Crinkle Leaf Plant, Key Lime Pie');
INSERT INTO
   a
VALUES
(
      'Sedum album ''Coral Carpet'' – White Stonecrop',
      'Key Lime ''plus'''
   );
CREATE TABLE [b] ([title] TEXT);

INSERT INTO
   b
VALUES
('Adromischus cristatus (Crinkle Leaf Plant)');

INSERT INTO
   b
VALUES
   ('Coral Carpet');


COMMIT;