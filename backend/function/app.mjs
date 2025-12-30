// =========================
// file: backend/function/app.mjs
// =========================
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);

const { v4: uuidv4 } = require("uuid");
const jwt = require("jsonwebtoken");
const crypto = require("node:crypto");

const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const { Ydb } = require("ydb-sdk-lite");

/**
 * Cloud Function handler for Yandex API Gateway.
 * Assumes OpenAPI routes all /api/* to this function (payload_format_version: "1.0").
 */

// ---- config ----
const JWT_SECRET = process.env.JWT_SECRET || "";
const YDB_DB_NAME = process.env.YDB_DB_NAME || "";
const TP = (process.env.YDB_TABLE_PREFIX || "").trim();

// Accept both names: LOGS_BUCKET (old) and RAW_BUCKET (new, used in GitHub vars)
const LOGS_BUCKET = process.env.LOGS_BUCKET || process.env.RAW_BUCKET || "";
const LOGS_PREFIX = (process.env.LOGS_PREFIX || "raw").replace(/\/+$/, "");

const S3_ENDPOINT = process.env.S3_ENDPOINT || "https://storage.yandexcloud.net";
const AWS_REGION = process.env.AWS_REGION || "ru-central1";

const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID || "";
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY || "";

// Auth delivery mode:
// - gateway: send code via Telegram Gateway (phone must be linked to Telegram)
// - mock: do not send anything, accept code 0000
const SMS_MODE = (process.env.SMS_MODE || "gateway").toLowerCase(); // gateway | mock

const TELEGRAM_GATEWAY_TOKEN = process.env.TELEGRAM_GATEWAY_TOKEN || "";
const TELEGRAM_GATEWAY_BASE_URL =
  (process.env.TELEGRAM_GATEWAY_BASE_URL || "https://gatewayapi.telegram.org/").replace(/\/+$/, "") + "/";
const TELEGRAM_GATEWAY_TTL = Number(process.env.TELEGRAM_GATEWAY_TTL || "600"); // seconds (30..3600)

const DEBUG_OTP = (process.env.DEBUG_OTP || "0") === "1";

// games catalog
const GAMES = [
  { game_id: "parabola", title: "Parabola" },
  { game_id: "balancer", title: "Balancer" },
  { game_id: "graph_master", title: "Graph Master" },
  { game_id: "chemical_detective", title: "Chemical Detective" },
  { game_id: "constructor", title: "Constructor" },
];

// ---- game constants ----
const GAME_MAX_STARS = {
  parabola: 18,
  balancer: 36,
  graph_master: 36,
  chemical_detective: 36,
  constructor: 36,
};
function getMaxStars(gameId) {
  return GAME_MAX_STARS[gameId] || 36;
}
function getExitThreshold(gameId) {
  // 50% от максимума (18->9, 36->18)
  return Math.floor(getMaxStars(gameId) / 2);
}

// ---- tests bank (source of truth) ----
// Structure: TEST_BANK[gameId][kind] -> array of questions
// Question: {id, text, pick, options:[{id,text}], correct:[optionId,...]}
const TEST_BANK = {
  "parabola": {
    "entry": [
      {
        "id": "q1",
        "text": "Какая физическая величина определяет форму траектории брошенного тела?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Масса тела"
          },
          {
            "id": "Б",
            "text": "Ускорение свободного падения"
          },
          {
            "id": "В",
            "text": "Цвет тела"
          },
          {
            "id": "Г",
            "text": "Температура воздуха"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q2",
        "text": "При каком угле броска тело достигнет максимальной высоты (при одинаковой начальной скорости)?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "30°"
          },
          {
            "id": "Б",
            "text": "45°"
          },
          {
            "id": "В",
            "text": "60°"
          },
          {
            "id": "Г",
            "text": "90°"
          }
        ],
        "correct": [
          "Г"
        ]
      },
      {
        "id": "q3",
        "text": "Как изменится дальность полёта тела, если увеличить начальную скорость в 2 раза (при постоянном угле)?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Увеличится в 2 раза"
          },
          {
            "id": "Б",
            "text": "Увеличится в 4 раза"
          },
          {
            "id": "В",
            "text": "Не изменится"
          },
          {
            "id": "Г",
            "text": "Уменьшится в 2 раза"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q4",
        "text": "Какие факторы влияют на траекторию полёта снаряда? (Выберите 2 варианта)",
        "multi": true,
        "pick": 2,
        "options": [
          {
            "id": "А",
            "text": "Угол броска"
          },
          {
            "id": "Б",
            "text": "Название снаряда"
          },
          {
            "id": "В",
            "text": "Сила тяжести"
          },
          {
            "id": "Г",
            "text": "День недели"
          }
        ],
        "correct": [
          "А",
          "В"
        ]
      },
      {
        "id": "q5",
        "text": "Тело брошено горизонтально. Как направлена его скорость в наивысшей точке траектории?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Вертикально вверх"
          },
          {
            "id": "Б",
            "text": "Горизонтально"
          },
          {
            "id": "В",
            "text": "Вертикально вниз"
          },
          {
            "id": "Г",
            "text": "Под углом 45° к горизонту"
          }
        ],
        "correct": [
          "Б"
        ]
      }
    ],
    "exit": [
      {
        "id": "q1",
        "text": "Что определяет максимальную высоту подъёма тела, брошенного под углом к горизонту?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Только угол броска"
          },
          {
            "id": "Б",
            "text": "Только начальная скорость"
          },
          {
            "id": "В",
            "text": "Угол броска и начальная скорость"
          },
          {
            "id": "Г",
            "text": "Масса тела"
          }
        ],
        "correct": [
          "В"
        ]
      },
      {
        "id": "q2",
        "text": "Под каким углом к горизонту нужно бросить тело, чтобы оно улетело дальше всего (при одинаковой начальной скорости)?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "30°"
          },
          {
            "id": "Б",
            "text": "45°"
          },
          {
            "id": "В",
            "text": "60°"
          },
          {
            "id": "Г",
            "text": "90°"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q3",
        "text": "Как попутный ветер влияет на траекторию снаряда?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Увеличивает дальность полёта"
          },
          {
            "id": "Б",
            "text": "Уменьшает дальность полёта"
          },
          {
            "id": "В",
            "text": "Увеличивает высоту подъёма"
          },
          {
            "id": "Г",
            "text": "Не влияет на траекторию"
          }
        ],
        "correct": [
          "А"
        ]
      },
      {
        "id": "q4",
        "text": "Какие величины остаются постоянными во время полёта снаряда в отсутствие ветра? (Выберите 2 варианта)",
        "multi": true,
        "pick": 2,
        "options": [
          {
            "id": "А",
            "text": "Горизонтальная составляющая скорости"
          },
          {
            "id": "Б",
            "text": "Вертикальная составляющая скорости"
          },
          {
            "id": "В",
            "text": "Ускорение свободного падения"
          },
          {
            "id": "Г",
            "text": "Высота над землёй"
          }
        ],
        "correct": [
          "А",
          "В"
        ]
      },
      {
        "id": "q5",
        "text": "Снаряд брошен вертикально вверх. Какова его скорость в наивысшей точке?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Максимальная"
          },
          {
            "id": "Б",
            "text": "Равна начальной"
          },
          {
            "id": "В",
            "text": "Равна нулю"
          },
          {
            "id": "Г",
            "text": "Направлена вниз"
          }
        ],
        "correct": [
          "В"
        ]
      }
    ]
  },
  "balancer": {
    "entry": [
      {
        "id": "q1",
        "text": "Что такое момент силы?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Произведение силы на время"
          },
          {
            "id": "Б",
            "text": "Произведение силы на плечо"
          },
          {
            "id": "В",
            "text": "Отношение силы к массе"
          },
          {
            "id": "Г",
            "text": "Сумма всех сил"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q2",
        "text": "В каких единицах измеряется момент силы в системе СИ?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Н (ньютон)"
          },
          {
            "id": "Б",
            "text": "Н·м (ньютон-метр)"
          },
          {
            "id": "В",
            "text": "кг·м/с² (килограмм-метр в секунду в квадрате)"
          },
          {
            "id": "Г",
            "text": "Дж (джоуль)"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q3",
        "text": "Рычаг находится в равновесии. Что верно для моментов сил относительно точки опоры?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Момент большей силы больше"
          },
          {
            "id": "Б",
            "text": "Моменты сил равны"
          },
          {
            "id": "В",
            "text": "Момент меньшей силы больше"
          },
          {
            "id": "Г",
            "text": "Моменты не связаны"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q4",
        "text": "На одном конце рычага груз 6 кг на расстоянии 2 м от оси. Какой груз нужен на расстоянии 3 м с другой стороны для равновесия?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "9 кг"
          },
          {
            "id": "Б",
            "text": "4 кг"
          },
          {
            "id": "В",
            "text": "6 кг"
          },
          {
            "id": "Г",
            "text": "12 кг"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q5",
        "text": "Какие факторы влияют на момент силы? (Выберите 2 варианта)",
        "multi": true,
        "pick": 2,
        "options": [
          {
            "id": "А",
            "text": "Величина силы"
          },
          {
            "id": "Б",
            "text": "Цвет груза"
          },
          {
            "id": "В",
            "text": "Расстояние до оси вращения"
          },
          {
            "id": "Г",
            "text": "Температура воздуха"
          }
        ],
        "correct": [
          "А",
          "В"
        ]
      }
    ],
    "exit": [
      {
        "id": "q1",
        "text": "Как называется расстояние от линии действия силы до оси вращения?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Рычаг"
          },
          {
            "id": "Б",
            "text": "Плечо силы"
          },
          {
            "id": "В",
            "text": "Момент"
          },
          {
            "id": "Г",
            "text": "Опора"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q2",
        "text": "Формула для вычисления момента силы:",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "M = F + d"
          },
          {
            "id": "Б",
            "text": "M = F / d"
          },
          {
            "id": "В",
            "text": "M = F × d"
          },
          {
            "id": "Г",
            "text": "M = F − d"
          }
        ],
        "correct": [
          "В"
        ]
      },
      {
        "id": "q3",
        "text": "Условие равновесия рычага:",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "F₁ = F₂"
          },
          {
            "id": "Б",
            "text": "d₁ = d₂"
          },
          {
            "id": "В",
            "text": "F₁ × d₁ = F₂ × d₂"
          },
          {
            "id": "Г",
            "text": "F₁ + F₂ = 0"
          }
        ],
        "correct": [
          "В"
        ]
      },
      {
        "id": "q4",
        "text": "Груз 4 кг находится на расстоянии 5 м от оси. Какой груз нужен на расстоянии 2 м для равновесия?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "8 кг"
          },
          {
            "id": "Б",
            "text": "10 кг"
          },
          {
            "id": "В",
            "text": "2 кг"
          },
          {
            "id": "Г",
            "text": "20 кг"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q5",
        "text": "Что произойдёт с рычагом, если увеличить плечо силы при постоянной силе? (Выберите 2 верных утверждения)",
        "multi": true,
        "pick": 2,
        "options": [
          {
            "id": "А",
            "text": "Момент силы увеличится"
          },
          {
            "id": "Б",
            "text": "Момент силы уменьшится"
          },
          {
            "id": "В",
            "text": "Легче повернуть рычаг"
          },
          {
            "id": "Г",
            "text": "Труднее повернуть рычаг"
          }
        ],
        "correct": [
          "А",
          "В"
        ]
      }
    ]
  },
  "chemical_detective": {
    "entry": [
      {
        "id": "q1",
        "text": "Какой цвет приобретает лакмус в кислой среде?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Синий"
          },
          {
            "id": "Б",
            "text": "Красный"
          },
          {
            "id": "В",
            "text": "Фиолетовый"
          },
          {
            "id": "Г",
            "text": "Зелёный"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q2",
        "text": "Что такое индикатор в химии?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Прибор для измерения температуры"
          },
          {
            "id": "Б",
            "text": "Вещество, меняющее цвет в зависимости от среды"
          },
          {
            "id": "В",
            "text": "Катализатор реакции"
          },
          {
            "id": "Г",
            "text": "Растворитель"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q3",
        "text": "Какое значение pH соответствует нейтральной среде?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "0"
          },
          {
            "id": "Б",
            "text": "7"
          },
          {
            "id": "В",
            "text": "14"
          },
          {
            "id": "Г",
            "text": "1"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q4",
        "text": "Какие вещества относятся к основаниям (щелочам)? (Выберите 2 варианта)",
        "multi": true,
        "pick": 2,
        "options": [
          {
            "id": "А",
            "text": "HNO₃"
          },
          {
            "id": "Б",
            "text": "KOH"
          },
          {
            "id": "В",
            "text": "Ca(OH)₂"
          },
          {
            "id": "Г",
            "text": "HCl"
          }
        ],
        "correct": [
          "Б",
          "В"
        ]
      },
      {
        "id": "q5",
        "text": "Какой металл окрашивает пламя в жёлтый цвет?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Калий"
          },
          {
            "id": "Б",
            "text": "Натрий"
          },
          {
            "id": "В",
            "text": "Медь"
          },
          {
            "id": "Г",
            "text": "Кальций"
          }
        ],
        "correct": [
          "Б"
        ]
      }
    ],
    "exit": [
      {
        "id": "q1",
        "text": "Какой цвет приобретает лакмус в щелочной среде?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Красный"
          },
          {
            "id": "Б",
            "text": "Синий"
          },
          {
            "id": "В",
            "text": "Жёлтый"
          },
          {
            "id": "Г",
            "text": "Бесцветный"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q2",
        "text": "Для чего используются индикаторы?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Для ускорения реакций"
          },
          {
            "id": "Б",
            "text": "Для определения кислотности/щёлочности среды"
          },
          {
            "id": "В",
            "text": "Для растворения веществ"
          },
          {
            "id": "Г",
            "text": "Для нагревания растворов"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q3",
        "text": "Раствор имеет pH = 3. Какая это среда?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Кислая"
          },
          {
            "id": "Б",
            "text": "Нейтральная"
          },
          {
            "id": "В",
            "text": "Щелочная"
          },
          {
            "id": "Г",
            "text": "Невозможно определить"
          }
        ],
        "correct": [
          "А"
        ]
      },
      {
        "id": "q4",
        "text": "Какие вещества относятся к кислотам? (Выберите 2 варианта)",
        "multi": true,
        "pick": 2,
        "options": [
          {
            "id": "А",
            "text": "HCl"
          },
          {
            "id": "Б",
            "text": "NaOH"
          },
          {
            "id": "В",
            "text": "H₂SO₄"
          },
          {
            "id": "Г",
            "text": "NaCl"
          }
        ],
        "correct": [
          "А",
          "В"
        ]
      },
      {
        "id": "q5",
        "text": "Какой металл окрашивает пламя в фиолетовый цвет?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Натрий"
          },
          {
            "id": "Б",
            "text": "Калий"
          },
          {
            "id": "В",
            "text": "Литий"
          },
          {
            "id": "Г",
            "text": "Барий"
          }
        ],
        "correct": [
          "Б"
        ]
      }
    ]
  },
  "constructor": {
    "entry": [
      {
        "id": "q1",
        "text": "Что такое валентность атома?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Масса атома"
          },
          {
            "id": "Б",
            "text": "Способность атома образовывать определённое число химических связей"
          },
          {
            "id": "В",
            "text": "Заряд ядра атома"
          },
          {
            "id": "Г",
            "text": "Число электронов в атоме"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q2",
        "text": "Какова валентность кислорода (O) в большинстве соединений?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "I"
          },
          {
            "id": "Б",
            "text": "II"
          },
          {
            "id": "В",
            "text": "III"
          },
          {
            "id": "Г",
            "text": "IV"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q3",
        "text": "Какова валентность углерода (C) в органических соединениях?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "I"
          },
          {
            "id": "Б",
            "text": "II"
          },
          {
            "id": "В",
            "text": "III"
          },
          {
            "id": "Г",
            "text": "IV"
          }
        ],
        "correct": [
          "Г"
        ]
      },
      {
        "id": "q4",
        "text": "Какие молекулы содержат двойные связи? (Выберите 2 варианта)",
        "multi": true,
        "pick": 2,
        "options": [
          {
            "id": "А",
            "text": "H₂ (водород)"
          },
          {
            "id": "Б",
            "text": "CO₂ (углекислый газ)"
          },
          {
            "id": "В",
            "text": "CH₄ (метан)"
          },
          {
            "id": "Г",
            "text": "C₂H₄ (этилен)"
          }
        ],
        "correct": [
          "Б",
          "Г"
        ]
      },
      {
        "id": "q5",
        "text": "Сколько атомов водорода в молекуле воды (H₂O)?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "1"
          },
          {
            "id": "Б",
            "text": "2"
          },
          {
            "id": "В",
            "text": "3"
          },
          {
            "id": "Г",
            "text": "4"
          }
        ],
        "correct": [
          "Б"
        ]
      }
    ],
    "exit": [
      {
        "id": "q1",
        "text": "Валентность показывает:",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Количество протонов в ядре"
          },
          {
            "id": "Б",
            "text": "Число связей, которые может образовать атом"
          },
          {
            "id": "В",
            "text": "Массу молекулы"
          },
          {
            "id": "Г",
            "text": "Температуру плавления вещества"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q2",
        "text": "Какова валентность водорода (H) в большинстве соединений?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "I"
          },
          {
            "id": "Б",
            "text": "II"
          },
          {
            "id": "В",
            "text": "III"
          },
          {
            "id": "Г",
            "text": "IV"
          }
        ],
        "correct": [
          "А"
        ]
      },
      {
        "id": "q3",
        "text": "Какова валентность азота (N) в молекуле аммиака (NH₃)?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "I"
          },
          {
            "id": "Б",
            "text": "II"
          },
          {
            "id": "В",
            "text": "III"
          },
          {
            "id": "Г",
            "text": "IV"
          }
        ],
        "correct": [
          "В"
        ]
      },
      {
        "id": "q4",
        "text": "Какие типы химических связей существуют в молекулах? (Выберите 2 варианта)",
        "multi": true,
        "pick": 2,
        "options": [
          {
            "id": "А",
            "text": "Одинарная связь"
          },
          {
            "id": "Б",
            "text": "Магнитная связь"
          },
          {
            "id": "В",
            "text": "Двойная связь"
          },
          {
            "id": "Г",
            "text": "Звуковая связь"
          }
        ],
        "correct": [
          "А",
          "В"
        ]
      },
      {
        "id": "q5",
        "text": "Сколько атомов водорода в молекуле метана (CH₄)?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "1"
          },
          {
            "id": "Б",
            "text": "2"
          },
          {
            "id": "В",
            "text": "3"
          },
          {
            "id": "Г",
            "text": "4"
          }
        ],
        "correct": [
          "Г"
        ]
      }
    ]
  },
  "graph_master": {
    "entry": [
      {
        "id": "q1",
        "text": "Как называется график линейной функции y = kx + b?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Парабола"
          },
          {
            "id": "Б",
            "text": "Прямая"
          },
          {
            "id": "В",
            "text": "Гипербола"
          },
          {
            "id": "Г",
            "text": "Окружность"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q2",
        "text": "Что показывает коэффициент k в уравнении y = kx + b?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Точку пересечения с осью Y"
          },
          {
            "id": "Б",
            "text": "Угол наклона прямой"
          },
          {
            "id": "В",
            "text": "Длину отрезка"
          },
          {
            "id": "Г",
            "text": "Площадь фигуры"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q3",
        "text": "Как называется график квадратичной функции y = ax²?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Прямая"
          },
          {
            "id": "Б",
            "text": "Окружность"
          },
          {
            "id": "В",
            "text": "Парабола"
          },
          {
            "id": "Г",
            "text": "Эллипс"
          }
        ],
        "correct": [
          "В"
        ]
      },
      {
        "id": "q4",
        "text": "Какие утверждения верны для функции y = x + 3? (Выберите 2 варианта)",
        "multi": true,
        "pick": 2,
        "options": [
          {
            "id": "А",
            "text": "График проходит через начало координат"
          },
          {
            "id": "Б",
            "text": "График пересекает ось Y в точке (0; 3)"
          },
          {
            "id": "В",
            "text": "Угловой коэффициент равен 1"
          },
          {
            "id": "Г",
            "text": "Угловой коэффициент равен 3"
          }
        ],
        "correct": [
          "Б",
          "В"
        ]
      },
      {
        "id": "q5",
        "text": "Что означает параметр b в уравнении y = kx + b?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Угол наклона"
          },
          {
            "id": "Б",
            "text": "Сдвиг графика по оси Y"
          },
          {
            "id": "В",
            "text": "Масштаб по оси X"
          },
          {
            "id": "Г",
            "text": "Точку минимума"
          }
        ],
        "correct": [
          "Б"
        ]
      }
    ],
    "exit": [
      {
        "id": "q1",
        "text": "Как выглядит график функции y = 2x?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Парабола"
          },
          {
            "id": "Б",
            "text": "Прямая, проходящая через начало координат"
          },
          {
            "id": "В",
            "text": "Горизонтальная прямая"
          },
          {
            "id": "Г",
            "text": "Вертикальная прямая"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q2",
        "text": "Если k > 0 в уравнении y = kx + b, то прямая:",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Убывает слева направо"
          },
          {
            "id": "Б",
            "text": "Возрастает слева направо"
          },
          {
            "id": "В",
            "text": "Параллельна оси X"
          },
          {
            "id": "Г",
            "text": "Параллельна оси Y"
          }
        ],
        "correct": [
          "Б"
        ]
      },
      {
        "id": "q3",
        "text": "Ветви параболы y = ax² направлены вверх, если:",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "a > 0"
          },
          {
            "id": "Б",
            "text": "a < 0"
          },
          {
            "id": "В",
            "text": "a = 0"
          },
          {
            "id": "Г",
            "text": "a = 1"
          }
        ],
        "correct": [
          "А"
        ]
      },
      {
        "id": "q4",
        "text": "Какие утверждения верны для функции y = x² − 4? (Выберите 2 варианта)",
        "multi": true,
        "pick": 2,
        "options": [
          {
            "id": "А",
            "text": "Вершина параболы в точке (0; −4)"
          },
          {
            "id": "Б",
            "text": "Вершина параболы в точке (0; 4)"
          },
          {
            "id": "В",
            "text": "Ветви направлены вверх"
          },
          {
            "id": "Г",
            "text": "Ветви направлены вниз"
          }
        ],
        "correct": [
          "А",
          "В"
        ]
      },
      {
        "id": "q5",
        "text": "Что означает параметр c в уравнении y = x² + c?",
        "multi": false,
        "pick": 1,
        "options": [
          {
            "id": "А",
            "text": "Угол наклона"
          },
          {
            "id": "Б",
            "text": "Вертикальный сдвиг параболы"
          },
          {
            "id": "В",
            "text": "Горизонтальный сдвиг параболы"
          },
          {
            "id": "Г",
            "text": "Ширину параболы"
          }
        ],
        "correct": [
          "Б"
        ]
      }
    ]
  }
};

function stripCorrect(questions) {
  return (questions || []).map((q) => ({
    id: q.id,
    text: q.text,
    pick: Number(q.pick || 1),
    options: (q.options || []).map((o) => ({ id: o.id, text: o.text })),
  }));
}

function validateAndScoreTest(gameId, kind, answers) {
  const bank = TEST_BANK?.[gameId]?.[kind];
  if (!Array.isArray(bank)) return { ok: false, error: "test_not_found" };

  const a = (answers && typeof answers === "object") ? answers : {};
  let score = 0;
  const maxScore = bank.length * 2;
  const correctMap = {};

  for (const q of bank) {
    const qid = q.id;
    const pick = Number(q.pick || 1);
    const correct = Array.isArray(q.correct) ? q.correct : [];
    correctMap[qid] = correct.slice();

    const selRaw = a[qid];
    const sel = Array.isArray(selRaw) ? selRaw.map(String) : (selRaw ? [String(selRaw)] : []);
    const uniq = Array.from(new Set(sel));

    // validate ids exist
    const validIds = new Set((q.options || []).map((o) => String(o.id)));
    if (uniq.some((x) => !validIds.has(String(x)))) {
      return { ok: false, error: "invalid_option", qid };
    }

    if (uniq.length !== pick) {
      return { ok: false, error: "invalid_pick", qid, expected: pick, got: uniq.length };
    }

    if (pick === 1) {
      if (uniq[0] === String(correct[0] || "")) score += 2;
    } else {
      // per correct option: 1 point, max 2
      for (const s of uniq) {
        if (correct.includes(s)) score += 1;
      }
    }
  }

  return { ok: true, score, max_score: maxScore, correct: correctMap };
}


// ---- clients (lazy singletons) ----
let ydb = null;
let s3 = null;
let schemaEnsured = false;

function normalizePathname(p) {
  if (!p) return "/";
  if (p.length > 1 && p.endsWith("/")) return p.slice(0, -1);
  return p;
}

function resp(statusCode, obj, extraHeaders) {
  return {
    statusCode,
    isBase64Encoded: false,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
      "Cache-Control": "no-store",
      ...(extraHeaders || {}),
    },
    body: JSON.stringify(obj),
  };
}

function normalizePhone(p) {
  const s = String(p || "").trim();
  const digits = s.replace(/\D/g, "");

  // RU only: accept +7XXXXXXXXXX / 7XXXXXXXXXX / 8XXXXXXXXXX / XXXXXXXXXX
  let national = "";
  if (digits.length === 11 && (digits.startsWith("7") || digits.startsWith("8"))) national = digits.slice(1);
  else if (digits.length === 10) national = digits;
  else return null;

  if (national.length !== 10) return null;
  return "+7" + national;
}

function sha256(s) {
  return crypto.createHash("sha256").update(String(s)).digest("hex");
}

function calcStarsTotalFromSummary(summary) {
  if (!summary || typeof summary !== "object") return 0;
  const v = summary.stars_total;
  if (Number.isFinite(v)) return Math.max(0, Math.floor(v));
  if (Array.isArray(summary.stars_by_level)) {
    return summary.stars_by_level.reduce((a, b) => a + (Number(b) || 0), 0);
  }
  return 0;
}

function parseEvent(event) {
  const method = event.httpMethod || event?.requestContext?.httpMethod || "GET";
  const path = normalizePathname(event.path || event.url || "/");
  const headers = event.headers || {};
  const qs = event.queryStringParameters || {};
  const pathParams =
    event.pathParameters || event.pathParams || event.params || event.parameters || {};

  const rawBody = event.body || "";
  const decodedBody = rawBody
    ? event.isBase64Encoded
      ? Buffer.from(rawBody, "base64").toString("utf-8")
      : rawBody
    : "";

  const body = decodedBody
    ? (() => {
        try {
          return JSON.parse(decodedBody);
        } catch {
          return null;
        }
      })()
    : null;

  return { method, path, headers, qs, pathParams, body };
}

function getBearer(headers) {
  const h = headers?.Authorization || headers?.authorization || "";
  const m = String(h).match(/^Bearer\s+(.+)$/i);
  return m ? m[1] : "";
}

function verifyToken(token) {
  if (!token || !JWT_SECRET) return null;
  try {
    return jwt.verify(token, JWT_SECRET);
  } catch {
    return null;
  }
}

function signToken(user) {
  if (!JWT_SECRET) throw new Error("JWT_SECRET is required");
  return jwt.sign(
    { user_id: user.user_id, phone: user.phone, name: user.name || "" },
    JWT_SECRET,
    { expiresIn: "30d" }
  );
}

async function getYdb(context) {
  if (ydb) return ydb;
  if (!YDB_DB_NAME) throw new Error("YDB_DB_NAME is required");

  // Cloud Functions: context.token is an object: { access_token, expires_in, token_type }
  const iamToken =
    (context?.token && typeof context.token === "object" ? context.token.access_token : "") ||
    (typeof context?.access_token === "string" ? context.access_token : "") ||
    (typeof context?.token === "string" ? context.token : "") ||
    process.env.YDB_IAM_TOKEN ||
    "";

  if (!iamToken) {
    console.log("No IAM token in context. context.token type =", typeof context?.token);
    throw new Error("No IAM token for YDB");
  }

  // ydb-sdk-lite expects iamToken (no manual 'Bearer ')
  ydb = new Ydb({ dbName: YDB_DB_NAME, iamToken });
  return ydb;
}

function getS3() {
  if (s3) return s3;
  s3 = new S3Client({
    region: AWS_REGION,
    endpoint: S3_ENDPOINT,
    credentials:
      AWS_ACCESS_KEY_ID && AWS_SECRET_ACCESS_KEY
        ? { accessKeyId: AWS_ACCESS_KEY_ID, secretAccessKey: AWS_SECRET_ACCESS_KEY }
        : undefined,
  });
  return s3;
}

// Telegram Gateway only (no Yandex Cloud Notification Service / SMS)

async function ensureSchema(context) {
  if (schemaEnsured) return;
  const y = await getYdb(context);

  const yql = `
    CREATE TABLE IF NOT EXISTS ${TP}users (
      user_id Utf8,
      phone Utf8,
      name Utf8,
      created_at Timestamp,
      PRIMARY KEY (user_id)
    );

    CREATE TABLE IF NOT EXISTS ${TP}auth_codes (
      phone Utf8,
      code_hash Utf8,
      expires_at Timestamp,
      created_at Timestamp,
      PRIMARY KEY (phone)
    );

    CREATE TABLE IF NOT EXISTS ${TP}game_stats (
      user_id Utf8,
      game_id Utf8,
      last_stars Int32,
      best_stars Int32,
      last_updated_at Timestamp,
      PRIMARY KEY (user_id, game_id)
    );

    CREATE TABLE IF NOT EXISTS ${TP}saves (
      user_id Utf8,
      game_id Utf8,
      payload_json Utf8,
      updated_at Timestamp,
      PRIMARY KEY (user_id, game_id)
    );

    CREATE TABLE IF NOT EXISTS ${TP}sessions (
      session_id Utf8,
      user_id Utf8,
      game_id Utf8,
      started_at Timestamp,
      finished_at Optional<Timestamp>,
      reason Utf8,
      summary_json Utf8,
      stars_total Int32,
      raw_key Utf8,
      PRIMARY KEY (session_id)
    );

    CREATE TABLE IF NOT EXISTS ${TP}tests_results (
      user_id Utf8,
      game_id Utf8,
      test_type Utf8, -- entry | exit
      score Int32,
      max_score Int32,
      answers_json Utf8,
      details_json Utf8,
      taken_at Timestamp,
      PRIMARY KEY (user_id, game_id, test_type)
    );
`;
  try {
    await y.executeYql(yql);
  } catch (e) {
    // If schema already exists or SA cannot create tables, we allow continuing.
    console.log("ensureSchema warning:", String(e?.message || e));
  }
  schemaEnsured = true;
}

// ---- DB helpers (ALL PARAM QUERIES USE DECLARE) ----

async function dbGetUserByPhone(context, phone) {
  const y = await getYdb(context);
  const q = `
    DECLARE $phone AS Utf8;
    SELECT user_id, phone, name, created_at
    FROM ${TP}users
    WHERE phone = $phone
    LIMIT 1;
  `;
  const [rows] = await y.executeDataQuery(q, { $phone: phone });
  return rows?.[0] || null;
}

async function dbGetUserById(context, userId) {
  const y = await getYdb(context);
  const q = `
    DECLARE $uid AS Utf8;
    SELECT user_id, phone, name, created_at
    FROM ${TP}users
    WHERE user_id = $uid
    LIMIT 1;
  `;
  const [rows] = await y.executeDataQuery(q, { $uid: userId });
  return rows?.[0] || null;
}

async function dbCreateUser(context, phone) {
  const y = await getYdb(context);
  const user = { user_id: uuidv4(), phone, name: "" };
  const q = `
    DECLARE $uid AS Utf8;
    DECLARE $phone AS Utf8;
    DECLARE $name AS Utf8;

    UPSERT INTO ${TP}users (user_id, phone, name, created_at)
    VALUES ($uid, $phone, $name, CurrentUtcTimestamp());
  `;
  await y.executeDataQuery(q, { $uid: user.user_id, $phone: phone, $name: "" });
  return user;
}

async function dbUpdateUserName(context, userId, name) {
  const y = await getYdb(context);
  const q = `
    DECLARE $uid AS Utf8;
    DECLARE $name AS Utf8;

    UPDATE ${TP}users
    SET name = $name
    WHERE user_id = $uid;
  `;
  await y.executeDataQuery(q, { $uid: userId, $name: name });
}

async function dbPutAuthCode(context, phone, codeHash, expiresAtIso) {
  const y = await getYdb(context);
  const q = `
    DECLARE $phone AS Utf8;
    DECLARE $hash AS Utf8;
    DECLARE $exp AS Utf8;

    UPSERT INTO ${TP}auth_codes (phone, code_hash, expires_at, created_at)
    VALUES ($phone, $hash, CAST($exp AS Timestamp), CurrentUtcTimestamp());
  `;
  await y.executeDataQuery(q, { $phone: phone, $hash: codeHash, $exp: expiresAtIso });
}

async function dbGetAuthCode(context, phone) {
  const y = await getYdb(context);
  const q = `
    DECLARE $phone AS Utf8;
    SELECT phone, code_hash, expires_at
    FROM ${TP}auth_codes
    WHERE phone = $phone
    LIMIT 1;
  `;
  const [rows] = await y.executeDataQuery(q, { $phone: phone });
  return rows?.[0] || null;
}

async function dbDeleteAuthCode(context, phone) {
  const y = await getYdb(context);
  const q = `
    DECLARE $phone AS Utf8;
    DELETE FROM ${TP}auth_codes
    WHERE phone = $phone;
  `;
  await y.executeDataQuery(q, { $phone: phone });
}

async function dbGetStatsAndSaves(context, userId) {
  const y = await getYdb(context);

  const q1 = `
    DECLARE $uid AS Utf8;
    SELECT game_id, last_stars, best_stars
    FROM ${TP}game_stats
    WHERE user_id = $uid;
  `;
  const [statsRows] = await y.executeDataQuery(q1, { $uid: userId });

  const q2 = `
    DECLARE $uid AS Utf8;
    SELECT game_id
    FROM ${TP}saves
    WHERE user_id = $uid;
  `;
  const [saveRows] = await y.executeDataQuery(q2, { $uid: userId });

  const stats = {};
  for (const r of statsRows || []) {
    stats[r.game_id] = {
      last_stars: Number(r.last_stars || 0),
      best_stars: Number(r.best_stars || 0),
    };
  }
  const saves = new Set((saveRows || []).map((r) => r.game_id));
  return { stats, saves };
}



// ---- tests results ----

async function dbGetTestResultsByUser(context, userId) {
  const y = await getYdb(context);
  const q = `
    DECLARE $uid AS Utf8;
    SELECT game_id, test_type, score, max_score, taken_at
    FROM ${TP}tests_results
    WHERE user_id = $uid;
  `;
  const [rows] = await y.executeDataQuery(q, { $uid: userId });
  const out = {};
  for (const r of rows || []) {
    const gid = String(r.game_id);
    const tt = String(r.test_type);
    out[gid] = out[gid] || {};
    out[gid][tt] = {
      score: Number(r.score || 0),
      max_score: Number(r.max_score || 0),
      taken_at: r.taken_at,
    };
  }
  return out;
}

async function dbGetTestResult(context, userId, gameId, testType) {
  const y = await getYdb(context);
  const q = `
    DECLARE $uid AS Utf8;
    DECLARE $gid AS Utf8;
    DECLARE $tt AS Utf8;
    SELECT score, max_score, answers_json, details_json, taken_at
    FROM ${TP}tests_results
    WHERE user_id = $uid AND game_id = $gid AND test_type = $tt
    LIMIT 1;
  `;
  const [rows] = await y.executeDataQuery(q, { $uid: userId, $gid: gameId, $tt: testType });
  const r = rows?.[0];
  if (!r) return null;

  let answers = null;
  let details = null;
  try { answers = r.answers_json ? JSON.parse(r.answers_json) : null; } catch { answers = null; }
  try { details = r.details_json ? JSON.parse(r.details_json) : null; } catch { details = null; }

  return {
    score: Number(r.score || 0),
    max_score: Number(r.max_score || 0),
    taken_at: r.taken_at,
    answers,
    details,
  };
}

async function dbPutTestResult(context, userId, gameId, testType, score, maxScore, answersObj, detailsObj) {
  const y = await getYdb(context);
  const q = `
    DECLARE $uid AS Utf8;
    DECLARE $gid AS Utf8;
    DECLARE $tt AS Utf8;
    DECLARE $score AS Int32;
    DECLARE $max AS Int32;
    DECLARE $answers AS Utf8;
    DECLARE $details AS Utf8;

    UPSERT INTO ${TP}tests_results (user_id, game_id, test_type, score, max_score, answers_json, details_json, taken_at)
    VALUES ($uid, $gid, $tt, $score, $max, $answers, $details, CurrentUtcTimestamp());
  `;
  await y.executeDataQuery(q, {
    $uid: userId,
    $gid: gameId,
    $tt: testType,
    $score: Math.floor(Number(score || 0)),
    $max: Math.floor(Number(maxScore || 0)),
    $answers: JSON.stringify(answersObj || {}),
    $details: JSON.stringify(detailsObj || {}),
  });
}

async function dbGetBestStars(context, userId, gameId) {
  const y = await getYdb(context);
  const q = `
    DECLARE $uid AS Utf8;
    DECLARE $gid AS Utf8;
    SELECT best_stars
    FROM ${TP}game_stats
    WHERE user_id = $uid AND game_id = $gid
    LIMIT 1;
  `;
  const [rows] = await y.executeDataQuery(q, { $uid: userId, $gid: gameId });
  const r = rows?.[0];
  return Number(r?.best_stars || 0);
}


async function dbHasGameStats(context, userId, gameId) {
  const y = await getYdb(context);
  const q = `
    DECLARE $uid AS Utf8;
    DECLARE $gid AS Utf8;
    SELECT 1 AS ok
    FROM ${TP}game_stats
    WHERE user_id = $uid AND game_id = $gid
    LIMIT 1;
  `;
  const [rows] = await y.executeDataQuery(q, { $uid: userId, $gid: gameId });
  return !!rows?.[0];
}

async function dbSaveGet(context, userId, gameId) {
  const y = await getYdb(context);
  const q = `
    DECLARE $uid AS Utf8;
    DECLARE $gid AS Utf8;

    SELECT payload_json, updated_at
    FROM ${TP}saves
    WHERE user_id = $uid AND game_id = $gid
    LIMIT 1;
  `;
  const [rows] = await y.executeDataQuery(q, { $uid: userId, $gid: gameId });
  const r = rows?.[0];
  if (!r) return null;

  let payload = null;
  try {
    payload = JSON.parse(r.payload_json);
  } catch {
    payload = null;
  }
  return { payload, updated_at: r.updated_at };
}

async function dbSavePut(context, userId, gameId, payload) {
  const y = await getYdb(context);
  const q = `
    DECLARE $uid AS Utf8;
    DECLARE $gid AS Utf8;
    DECLARE $p AS Utf8;

    UPSERT INTO ${TP}saves (user_id, game_id, payload_json, updated_at)
    VALUES ($uid, $gid, $p, CurrentUtcTimestamp());
  `;
  await y.executeDataQuery(q, {
    $uid: userId,
    $gid: gameId,
    $p: JSON.stringify(payload),
  });
}

async function dbSaveDelete(context, userId, gameId) {
  const y = await getYdb(context);
  const q = `
    DECLARE $uid AS Utf8;
    DECLARE $gid AS Utf8;

    DELETE FROM ${TP}saves
    WHERE user_id = $uid AND game_id = $gid;
  `;
  await y.executeDataQuery(q, { $uid: userId, $gid: gameId });
}

async function dbSessionStart(context, sessionId, userId, gameId) {
  const y = await getYdb(context);
  const q = `
    DECLARE $sid AS Utf8;
    DECLARE $uid AS Utf8;
    DECLARE $gid AS Utf8;

    UPSERT INTO ${TP}sessions (session_id, user_id, game_id, started_at, reason, summary_json, stars_total, raw_key)
    VALUES ($sid, $uid, $gid, CurrentUtcTimestamp(), "", "{}", 0, "");
  `;
  await y.executeDataQuery(q, { $sid: sessionId, $uid: userId, $gid: gameId });
}

async function dbSessionGet(context, sessionId) {
  const y = await getYdb(context);
  const q = `
    DECLARE $sid AS Utf8;
    SELECT session_id, user_id, game_id
    FROM ${TP}sessions
    WHERE session_id = $sid
    LIMIT 1;
  `;
  const [rows] = await y.executeDataQuery(q, { $sid: sessionId });
  return rows?.[0] || null;
}

async function dbSessionFinish(context, sessionId, reason, summary, starsTotal, rawKey) {
  const y = await getYdb(context);
  const q = `
    DECLARE $sid AS Utf8;
    DECLARE $reason AS Utf8;
    DECLARE $summary AS Utf8;
    DECLARE $stars AS Int32;
    DECLARE $raw AS Utf8;

    UPDATE ${TP}sessions
    SET finished_at = CurrentUtcTimestamp(),
        reason = $reason,
        summary_json = $summary,
        stars_total = $stars,
        raw_key = $raw
    WHERE session_id = $sid;
  `;
  await y.executeDataQuery(q, {
    $sid: sessionId,
    $reason: reason,
    $summary: JSON.stringify(summary || {}),
    $stars: Math.floor(starsTotal || 0),
    $raw: rawKey || "",
  });
}

async function dbUpsertGameStats(context, userId, gameId, starsTotal) {
  const y = await getYdb(context);

  const q1 = `
    DECLARE $uid AS Utf8;
    DECLARE $gid AS Utf8;

    SELECT last_stars, best_stars
    FROM ${TP}game_stats
    WHERE user_id = $uid AND game_id = $gid
    LIMIT 1;
  `;
  const [rows] = await y.executeDataQuery(q1, { $uid: userId, $gid: gameId });
  const cur = rows?.[0];
  const best = Math.max(Number(cur?.best_stars || 0), Math.floor(starsTotal || 0));

  const q2 = `
    DECLARE $uid AS Utf8;
    DECLARE $gid AS Utf8;
    DECLARE $last AS Int32;
    DECLARE $best AS Int32;

    UPSERT INTO ${TP}game_stats (user_id, game_id, last_stars, best_stars, last_updated_at)
    VALUES ($uid, $gid, $last, $best, CurrentUtcTimestamp());
  `;
  await y.executeDataQuery(q2, {
    $uid: userId,
    $gid: gameId,
    $last: Math.floor(starsTotal || 0),
    $best: best,
  });

  return { last_stars: Math.floor(starsTotal || 0), best_stars: best };
}

// ---- integrations ----
async function sendOtp(phone, code) {
  if (SMS_MODE === "mock") {
    console.log("[OTP MOCK]", phone, code);
    return { ok: true };
  }

  if (SMS_MODE !== "gateway") {
    throw new Error(`unsupported_SMS_MODE_${SMS_MODE}`);
  }

  return await sendTelegramVerification(phone, code);
}

async function sendTelegramVerification(phone, code) {
  if (!TELEGRAM_GATEWAY_TOKEN) throw new Error("TELEGRAM_GATEWAY_TOKEN_missing");

  const ttlRaw = Number(TELEGRAM_GATEWAY_TTL);
  const ttl = Number.isFinite(ttlRaw) ? Math.min(3600, Math.max(30, ttlRaw)) : 600;

  const url = TELEGRAM_GATEWAY_BASE_URL + "sendVerificationMessage";
  const r = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${TELEGRAM_GATEWAY_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      phone_number: phone,
      code: String(code),
      ttl,
    }),
  });

  let j = null;
  try {
    j = await r.json();
  } catch {
    j = null;
  }

  if (!r.ok) {
    const msg = j?.error ? String(j.error) : `gateway_http_${r.status}`;
    throw new Error(msg);
  }
  if (!j?.ok) {
    throw new Error(j?.error ? String(j.error) : "gateway_error");
  }

  return { ok: true, request_id: j?.result?.request_id };
}

async function uploadRaw(gameId, userId, sessionId, events, summary) {
  if (!LOGS_BUCKET) return { ok: false, error: "LOGS_BUCKET not set" };

  const dt = new Date().toISOString().slice(0, 10);
  const key = `${LOGS_PREFIX}/game=${gameId}/dt=${dt}/user=${userId}/session=${sessionId}.jsonl`;

  const lines = [];
  lines.push(
    JSON.stringify({
      type: "meta",
      t: Date.now(),
      game_id: gameId,
      user_id: userId,
      session_id: sessionId,
    })
  );
  for (const e of Array.isArray(events) ? events : []) lines.push(JSON.stringify(e));
  lines.push(JSON.stringify({ type: "summary", t: Date.now(), summary: summary || {} }));

  const client = getS3();
  await client.send(
    new PutObjectCommand({
      Bucket: LOGS_BUCKET,
      Key: key,
      Body: Buffer.from(lines.join("\n") + "\n", "utf-8"),
      ContentType: "application/x-ndjson",
    })
  );

  return { ok: true, key };
}

// ---- handler ----
export async function handler(event, context) {
  try {
    const { method, path, headers, body } = parseEvent(event);

    // CORS preflight
    if (method === "OPTIONS") return resp(204, { ok: true });

    await ensureSchema(context);

    // /api/auth/start
    if (method === "POST" && path === "/api/auth/start") {
      const phone = normalizePhone(body?.phone);
      if (!phone) return resp(400, { ok: false, error: "phone_required" });

      const code = SMS_MODE === "mock" ? "0000" : String(Math.floor(1000 + Math.random() * 9000));
      const expiresAt = new Date(Date.now() + 10 * 60 * 1000).toISOString();

      await dbPutAuthCode(context, phone, sha256(code), expiresAt);

      try {
        await sendOtp(phone, code);
      } catch (e) {
        // avoid leaving an auth code that wasn't delivered
        try {
          await dbDeleteAuthCode(context, phone);
        } catch {
          /* noop */
        }

        console.log("sendOtp error:", String(e?.message || e));
        return resp(500, {
          ok: false,
          error: SMS_MODE === "gateway" ? "telegram_failed" : "otp_failed",
          ...(DEBUG_OTP ? { debug_code: code, debug_details: String(e?.message || e) } : {}),
        });
      }

      return resp(200, { ok: true, ...(DEBUG_OTP ? { debug_code: code } : {}) });
    }

    // /api/auth/verify
    if (method === "POST" && path === "/api/auth/verify") {
      const phone = normalizePhone(body?.phone);
      const code = String(body?.code || "").trim();
      if (!phone || !code) return resp(400, { ok: false, error: "phone_and_code_required" });

      const rec = await dbGetAuthCode(context, phone);
      if (!rec) return resp(401, { ok: false, error: "code_invalid" });

      // expires_at may come as Date, string, or sdk-specific type; String() is acceptable here
      const expMs = Date.parse(String(rec.expires_at));
      if (Number.isFinite(expMs) && expMs < Date.now()) {
        await dbDeleteAuthCode(context, phone);
        return resp(401, { ok: false, error: "code_expired" });
      }

      if (rec.code_hash !== sha256(code)) return resp(401, { ok: false, error: "code_invalid" });

      await dbDeleteAuthCode(context, phone);

      let user = await dbGetUserByPhone(context, phone);
      if (!user) user = await dbCreateUser(context, phone);

      const token = signToken(user);
      return resp(200, { ok: true, token });
    }

    // Auth required below
    const token = getBearer(headers);
    const claims = verifyToken(token);
    if (!claims) return resp(401, { ok: false, error: "unauthorized" });

    const user = await dbGetUserById(context, claims.user_id);
    if (!user) return resp(401, { ok: false, error: "unauthorized" });

    // /api/me GET
    if (method === "GET" && path === "/api/me") {
      const { stats, saves } = await dbGetStatsAndSaves(context, user.user_id);
      const tests = await dbGetTestResultsByUser(context, user.user_id);

      const games = GAMES.map((g) => {
        const gid = g.game_id;
        const last = stats[gid]?.last_stars || 0;
        const best = stats[gid]?.best_stars || 0;
        const hasPlayed = Object.prototype.hasOwnProperty.call(stats, gid);

        const maxStars = getMaxStars(gid);
        const exitThreshold = getExitThreshold(gid);

        const entryDone = !!tests?.[gid]?.entry;
        const exitDone = !!tests?.[gid]?.exit;

        // Входной тест обязателен ТОЛЬКО до первой попытки (если нет статистики игры)
        const needsEntryTest = !entryDone && !hasPlayed;

        const canTakeExit = !exitDone && best >= exitThreshold;

        return {
          game_id: gid,
          title: g.title,
          last_stars: last,
          best_stars: best,
          has_save: saves.has(gid),

          max_stars: maxStars,
          exit_threshold: exitThreshold,

          entry_test_done: entryDone,
          exit_test_done: exitDone,
          needs_entry_test: needsEntryTest,
          can_take_exit_test: canTakeExit,

          entry_score: tests?.[gid]?.entry?.score ?? null,
          exit_score: tests?.[gid]?.exit?.score ?? null,
        };
      });

      return resp(200, {
        ok: true,
        user: { user_id: user.user_id, phone: user.phone, name: user.name || "", games },
      });
    }

    // /api/me POST (set name)
    if (method === "POST" && path === "/api/me") {
      const name = String(body?.name || "").trim();
      if (!name) return resp(400, { ok: false, error: "name_required" });
      await dbUpdateUserName(context, user.user_id, name);
      return resp(200, { ok: true });
    }

    // saves: /api/games/{gameId}/save
    {
      const m = path.match(/^\/api\/games\/([^\/]+)\/save$/);
      if (m) {
        const gameId = decodeURIComponent(m[1]);
        if (!GAMES.some((g) => g.game_id === gameId))
          return resp(404, { ok: false, error: "unknown_game" });

        if (method === "GET") {
          const s = await dbSaveGet(context, user.user_id, gameId);
          return resp(200, { ok: true, save: s ? { updated_at: s.updated_at, payload: s.payload } : null });
        }
        if (method === "PUT") {
          const payload = body?.save ?? body?.payload;
          if (!payload || typeof payload !== "object")
            return resp(400, { ok: false, error: "save_required" });
          await dbSavePut(context, user.user_id, gameId, payload);
          return resp(200, { ok: true });
        }
        if (method === "DELETE") {
          await dbSaveDelete(context, user.user_id, gameId);
          return resp(200, { ok: true });
        }
      }
    }

    
    // tests: /api/games/{gameId}/tests/{entry|exit}
    {
      const m = path.match(/^\/api\/games\/([^\/]+)\/tests\/(entry|exit)$/);
      if (m) {
        const gameId = decodeURIComponent(m[1]);
        const kind = m[2];

        if (!GAMES.some((g) => g.game_id === gameId))
          return resp(404, { ok: false, error: "unknown_game" });

        if (!TEST_BANK?.[gameId]?.[kind])
          return resp(404, { ok: false, error: "test_not_found" });

        if (method === "GET") {
          const existing = await dbGetTestResult(context, user.user_id, gameId, kind);
          if (existing) {
            return resp(200, {
              ok: true,
              done: true,
              result: { score: existing.score, max_score: existing.max_score, taken_at: existing.taken_at },
            });
          }

          const test = {
            game_id: gameId,
            kind,
            max_score: (TEST_BANK[gameId][kind].length || 0) * 2,
            questions: stripCorrect(TEST_BANK[gameId][kind]),
          };
          return resp(200, { ok: true, done: false, test });
        }
      }
    }

    // submit test: /api/games/{gameId}/tests/{entry|exit}/submit
    {
      const m = path.match(/^\/api\/games\/([^\/]+)\/tests\/(entry|exit)\/submit$/);
      if (m) {
        const gameId = decodeURIComponent(m[1]);
        const kind = m[2];

        if (!GAMES.some((g) => g.game_id === gameId))
          return resp(404, { ok: false, error: "unknown_game" });

        if (!TEST_BANK?.[gameId]?.[kind])
          return resp(404, { ok: false, error: "test_not_found" });

        if (method !== "POST") return resp(405, { ok: false, error: "method_not_allowed" });

        // Only one attempt
        const existing = await dbGetTestResult(context, user.user_id, gameId, kind);
        if (existing) return resp(409, { ok: false, error: "test_already_done" });

        // Exit test is locked by stars threshold
        if (kind === "exit") {
          const best = await dbGetBestStars(context, user.user_id, gameId);
          const need = getExitThreshold(gameId);
          if (best < need) {
            return resp(403, { ok: false, error: "exit_test_locked", best_stars: best, required_stars: need });
          }
        }

        const answers = body?.answers;
        const scored = validateAndScoreTest(gameId, kind, answers);
        if (!scored.ok) return resp(400, { ok: false, error: scored.error, details: scored });

        // Persist
        const details = { kind, game_id: gameId, scored_at: new Date().toISOString() };
        await dbPutTestResult(
          context,
          user.user_id,
          gameId,
          kind,
          scored.score,
          scored.max_score,
          answers || {},
          details
        );

        return resp(200, {
          ok: true,
          score: scored.score,
          max_score: scored.max_score,
          correct: scored.correct,
        });
      }
    }

// session start: /api/games/{gameId}/session/start
    {
      const m = path.match(/^\/api\/games\/([^\/]+)\/session\/start$/);
      if (method === "POST" && m) {
        const gameId = decodeURIComponent(m[1]);
        if (!GAMES.some((g) => g.game_id === gameId))
          return resp(404, { ok: false, error: "unknown_game" });

        // Entry test is required only before the very first game attempt
        const hasEntry = await dbGetTestResult(context, user.user_id, gameId, "entry");
        const hasStats = await dbHasGameStats(context, user.user_id, gameId);
        if (!hasEntry && !hasStats) {
          return resp(403, { ok: false, error: "entry_test_required" });
        }

        const sid = uuidv4();
        await dbSessionStart(context, sid, user.user_id, gameId);
        return resp(200, { ok: true, session_id: sid });
      }
    }

    // session finish: /api/games/{gameId}/session/{sessionId}/finish
    {
      const m = path.match(/^\/api\/games\/([^\/]+)\/session\/([^\/]+)\/finish$/);
      if (method === "POST" && m) {
        const gameId = decodeURIComponent(m[1]);
        const sid = decodeURIComponent(m[2]);
        if (!GAMES.some((g) => g.game_id === gameId))
          return resp(404, { ok: false, error: "unknown_game" });

        // IMPORTANT: verify the session belongs to this user and this game
        const sess = await dbSessionGet(context, sid);
        if (!sess || sess.user_id !== user.user_id || sess.game_id !== gameId) {
          return resp(404, { ok: false, error: "session_not_found" });
        }

        const reason = String(body?.reason || "exit");
        const summary = body?.summary || {};
        const events = Array.isArray(body?.events) ? body.events : [];

        const starsTotal = calcStarsTotalFromSummary(summary);

        const up = await uploadRaw(gameId, user.user_id, sid, events, summary);
        const rawKey = up.ok ? up.key : "";

        await dbSessionFinish(context, sid, reason, summary, starsTotal, rawKey);
        await dbUpsertGameStats(context, user.user_id, gameId, starsTotal);

        return resp(200, { ok: true, raw_key: rawKey, stars_total: starsTotal });
      }
    }

    return resp(404, { ok: false, error: "not_found" });
  } catch (e) {
    console.log("handler error:", e?.stack || e);
    return resp(500, { ok: false, error: "internal_error" });
  }
}