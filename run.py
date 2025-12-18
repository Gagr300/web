import os
import sys

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Запускаем gateway приложение
from gateway.gateway import app

if __name__ == '__main__':
    print("Запуск микросервисного приложения...")
    print("Доступно по адресу: http://localhost:5000")
    print("Нажмите Ctrl+C для остановки")

    # Создаем необходимые директории
    os.makedirs('./gateway/templates', exist_ok=True)
    os.makedirs('./gateway/static', exist_ok=True)

    app.run(host='0.0.0.0', port=5000, debug=True)