import math


class ColorUtils:
    @staticmethod
    def calculate_weighted_average_color(ingredients):
        """
        Вычисляет средневзвешенный цвет на основе количества ингредиентов

        Args:
            ingredients: список словарей с ключами 'color_rgb' и 'quantity'

        Returns:
            dict: {'r': int, 'g': int, 'b': int}
        """
        if not ingredients:
            return {'r': 255, 'g': 255, 'b': 255}

        total_weight = 0
        weighted_r = 0
        weighted_g = 0
        weighted_b = 0

        for ingredient in ingredients:
            color = ingredient.get('color_rgb', [255, 255, 255])
            weight = ingredient.get('quantity', 1)

            # Если цвет в формате RGBA, берем только RGB
            if len(color) >= 3:
                r, g, b = color[0], color[1], color[2]

                # Применяем вес
                weighted_r += r * weight
                weighted_g += g * weight
                weighted_b += b * weight
                total_weight += weight

        if total_weight > 0:
            avg_r = round(weighted_r / total_weight)
            avg_g = round(weighted_g / total_weight)
            avg_b = round(weighted_b / total_weight)

            # Ограничиваем значения 0-255
            avg_r = max(0, min(255, avg_r))
            avg_g = max(0, min(255, avg_g))
            avg_b = max(0, min(255, avg_b))

            return {'r': avg_r, 'g': avg_g, 'b': avg_b}

        return {'r': 255, 'g': 255, 'b': 255}

    @staticmethod
    def rgb_to_hex(r, g, b):
        """Преобразует RGB в HEX"""
        return f"#{r:02x}{g:02x}{b:02x}".upper()

    @staticmethod
    def get_color_name(rgb):
        """
        Возвращает приблизительное название цвета на основе его RGB значений
        """
        r, g, b = rgb['r'], rgb['g'], rgb['b']

        # Определяем доминирующий цвет
        max_val = max(r, g, b)
        min_val = min(r, g, b)

        # Если цвета близки друг к другу - это оттенки серого
        if max_val - min_val < 30:
            if max_val < 100:
                return "Темно-серый"
            elif max_val < 180:
                return "Серый"
            else:
                return "Светло-серый"

        # Определяем оттенок по доминирующему каналу
        if r > g and r > b:
            if g > b:
                return "Оранжевый" if r > 200 else "Коричневый"
            else:
                return "Розовый" if r > 200 else "Бордовый"
        elif g > r and g > b:
            if r > b:
                return "Лаймовый" if g > 200 else "Оливковый"
            else:
                return "Зеленый" if g > 200 else "Темно-зеленый"
        else:  # blue dominant
            if r > g:
                return "Фиолетовый" if b > 200 else "Пурпурный"
            else:
                return "Голубой" if b > 200 else "Темно-синий"

    @staticmethod
    def generate_cocktail_name(color_rgb, ingredients):
        """
        Генерирует креативное название для коктейля
        на основе цвета и ингредиентов
        """
        color_name = ColorUtils.get_color_name(color_rgb)
        main_ingredient = max(ingredients, key=lambda x: x['quantity']) if ingredients else None

        adjectives = {
            'Оранжевый': ['Тропический', 'Солнечный', 'Апельсиновый'],
            'Розовый': ['Романтичный', 'Нежный', 'Ягодный'],
            'Красный': ['Страстный', 'Яркий', 'Фруктовый'],
            'Зеленый': ['Освежающий', 'Травяной', 'Натуральный'],
            'Синий': ['Морской', 'Прохладный', 'Глубокий'],
            'Фиолетовый': ['Таинственный', 'Королевский', 'Виноградный'],
            'Коричневый': ['Шоколадный', 'Землистый', 'Пряный'],
            'Желтый': ['Солнечный', 'Цитрусовый', 'Энергичный']
        }

        # Выбираем прилагательное по цвету
        base_color = next((c for c in adjectives.keys() if color_name.startswith(c)), 'Уникальный')
        adjective = adjectives.get(base_color, ['Уникальный'])[0]

        # Добавляем название основного ингредиента
        if main_ingredient:
            ingredient_name = main_ingredient['name'].capitalize()
            if ingredient_name in ['Молоко', 'Сливки']:
                return f"{adjective} {ingredient_name.lower()}ный коктейль"
            else:
                return f"{adjective} {ingredient_name.lower()}овый микс"

        return f"{adjective} коктейль"