# Парсер компаний и вакансий с портала TrudVsem
Что реализуется в проекте:
  - Получение данных через API ТРУДВСЕМ.
  - Распределение исходных данных  на два отношения -- "Компании" и "Вакансии" (2NF).
  - Получение из БД таблиц с кодами и названиями МРИГО/ОКПДТР, а также с параметрами для сопоставления.
  - Cопоставление адресов вакансий с кодами МРИГО и имен вакансий с кодами ОКПДТР.
  - Выгрузка/обновление таблиц 'Компании' и дополненной таблицы "Вакансии" в БД.
  - Логирование скрипта. 
  - Измерение времени выполнения. 