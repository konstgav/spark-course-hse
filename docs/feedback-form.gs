/**
 * Создаёт Google Форму с анкетой обратной связи по курсу (см. feedback-form.md).
 *
 * Как запустить:
 *   1. https://script.google.com → «Новый проект», вставить этот файл целиком.
 *   2. Выбрать функцию createFeedbackForm → «Выполнить», разрешить доступ к Формам и Таблицам.
 *   3. Ссылки на форму появятся в «Журнале выполнения».
 */

const LESSONS = [
  '1. Стенд и Spark DataFrame API',
  '2. Загрузка данных: bronze и silver (Airflow, Iceberg, MERGE)',
  '3. Аналитика и витрины (Postgres, Metabase)',
  '4. Машинное обучение (sklearn, Spark ML, модель в Airflow)',
];

const MISSED = 'Не был(а) на занятии';

// Ответы дополнительно складываются в Google Таблицу, её можно выгрузить в Excel.
const CREATE_RESPONSE_SHEET = true;

function createFeedbackForm() {
  const form = FormApp.create('Анкета обратной связи по курсу');
  form
    .setDescription('Анкета анонимная, займёт 3 минуты. По ответам решаем, продолжать ли курс.')
    .setCollectEmail(false)
    .setLimitOneResponsePerUser(false) // иначе потребуется вход в Google-аккаунт
    .setAllowResponseEdits(false)
    .setShowLinkToRespondAgain(false)
    .setProgressBar(false)
    .setConfirmationMessage('Спасибо! Ваши ответы помогут сделать курс лучше.');

  // Только для аккаунтов Google Workspace; в личном аккаунте вход и так не требуется.
  try {
    form.setRequireLogin(false);
  } catch (e) {}

  // 1–3. Таблицы: строки — занятия, один ответ в строке.
  addGrid(form, '1. Насколько интересными были занятия?',
    ['Очень интересно', 'Скорее интересно', 'Скорее неинтересно', 'Неинтересно', MISSED]);
  addGrid(form, '2. Насколько понятным был материал?',
    ['Всё понятно', 'Понятно почти всё', 'Многое осталось непонятным', 'Почти ничего не понял(а)', MISSED]);
  addGrid(form, '3. Насколько полезным будет материал для вашей учёбы или работы?',
    ['Очень полезно', 'Скорее полезно', 'Скорее бесполезно', 'Бесполезно', MISSED]);

  addChoice(form, '4. Темп занятий', [
    'Слишком быстро, не успевал(а) выполнять задания',
    'Немного быстро',
    'В самый раз',
    'Немного медленно',
    'Слишком медленно, было скучно',
  ], false);

  addChoice(form, '5. Соотношение теории и практики', [
    'Хотелось бы больше теории',
    'В самый раз',
    'Хотелось бы больше практики',
  ], false);

  addCheckbox(form, '6. Что вызвало больше всего трудностей?', [
    'Установка стенда: Docker, conda, hosts-файл',
    'Spark DataFrame API и Spark SQL',
    'Iceberg и MERGE INTO',
    "Airflow и DAG'и",
    'SQL для витрин',
    'Metabase',
    'Машинное обучение: модели и метрики',
    'Git и работа с ноутбуками',
    'Трудностей не было',
  ]);

  addCheckbox(form, '7. Какие инструменты курса вы, скорее всего, будете использовать дальше?', [
    'Spark',
    'Apache Iceberg',
    'Airflow',
    'Metabase или другой BI',
    'sklearn / Spark ML',
    'Docker',
    'Пока не планирую использовать',
  ]);

  addChoice(form, '8. Ваш опыт работы с данными до курса', [
    'Почти не программировал(а)',
    'Знаю основы Python и SQL',
    'Регулярно работаю с данными: аналитика, отчёты, ML',
    'Работаю дата-инженером или разработчиком',
  ], false);

  addChoice(form, '9. Пошли бы вы на продолжение курса?', [
    'Да, обязательно',
    'Скорее да',
    'Скорее нет',
    'Нет',
  ], true);

  addCheckbox(form, '10. Какие темы интересны для продолжения?', [
    'Потоковая обработка: Kafka + Spark Structured Streaming',
    'Iceberg глубже: time travel, эволюция схемы, обслуживание таблиц',
    "Оптимизация Spark: партиционирование, join'ы, разбор медленных запросов",
    'Качество данных и тесты пайплайнов',
    'dbt и моделирование хранилища',
    'MLOps: MLflow, версионирование моделей, мониторинг',
    'Облако и Kubernetes',
    'LLM и работа с текстами на больших данных',
  ], 3).setHelpText('Выберите не больше трёх вариантов.');

  form.addParagraphTextItem()
    .setTitle('11. Пожелания и предложения')
    .setHelpText('Что понравилось, что улучшить, чего не хватило? Любые замечания по материалам, стенду и формату занятий.')
    .setRequired(false);

  if (CREATE_RESPONSE_SHEET) {
    const sheet = SpreadsheetApp.create('Анкета обратной связи по курсу (ответы)');
    form.setDestination(FormApp.DestinationType.SPREADSHEET, sheet.getId());
    Logger.log('Таблица с ответами: ' + sheet.getUrl());
  }

  // Формы, созданные скриптом, могут оказаться неопубликованными.
  if (typeof form.setPublished === 'function') {
    form.setPublished(true);
  }

  const publishedUrl = form.getPublishedUrl();
  Logger.log('Редактирование: ' + form.getEditUrl());
  Logger.log('Ссылка для слушателей: ' + publishedUrl);
  Logger.log('Короткая ссылка: ' + form.shortenFormUrl(publishedUrl));
}

function addGrid(form, title, columns) {
  return form.addGridItem()
    .setTitle(title)
    .setRows(LESSONS)
    .setColumns(columns)
    .setRequired(true); // обязательный ответ в каждой строке; для пропустивших есть столбец MISSED
}

function addChoice(form, title, choices, required) {
  return form.addMultipleChoiceItem()
    .setTitle(title)
    .setChoiceValues(choices)
    .setRequired(required);
}

function addCheckbox(form, title, choices, maxSelected) {
  const item = form.addCheckboxItem()
    .setTitle(title)
    .setChoiceValues(choices)
    .setRequired(false);
  if (maxSelected) {
    item.setValidation(FormApp.createCheckboxValidation()
      .requireSelectAtMost(maxSelected)
      .setHelpText('Выберите не больше ' + maxSelected + ' вариантов.')
      .build());
  }
  return item;
}
