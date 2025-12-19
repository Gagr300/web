from flask import Flask, render_template, request, jsonify, session, redirect, url_for, flash
import os
import sys

app = Flask(__name__,
            template_folder=os.path.join(os.path.dirname(__file__), 'templates'),
            static_folder=os.path.join(os.path.dirname(__file__), 'static'))
app.secret_key = 'your-secret-key-here-change-this-in-production'
app.config['SESSION_TYPE'] = 'filesystem'

# Добавляем пути для импорта сервисов
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from Product.ProductService import product_service
from Order.OrderService import order_service
from Payment.PaymentService import payment_service


@app.context_processor
def inject_global_data():
    def get_cart_total():
        cart = session.get('cart', {})
        return sum(cart.values()) if cart else 0

    def get_cart_items():
        return session.get('cart', {})

    def get_total_cost():
        cart = session.get('cart', {})
        if not cart:
            return 0
        total = 0
        for product_name, quantity in cart.items():
            product = product_service.get_product(product_name)
            if product:
                total += product.cost * quantity
        return total

    def get_current_time():
        from datetime import datetime
        return datetime.now().strftime('%d.%m.%Y %H:%M')

    return {
        'cart_items': get_cart_items,
        'total_cost': get_total_cost,
        'current_time': get_current_time()
    }


@app.route('/')
def product_list():
    """Главная страница со списком продуктов"""
    products = product_service.get_all_products()
    return render_template('product_list.html', products=products)


@app.route('/add_to_cart', methods=['POST'])
def add_to_cart():
    """Добавить продукт в корзину"""
    try:
        product_name = request.form.get('product_name', '').strip()
        quantity = int(request.form.get('quantity', 1))

        if not product_name:
            return jsonify({'success': False, 'message': 'Не указано название продукта'}), 400

        product = product_service.get_product(product_name)
        if not product:
            return jsonify({'success': False, 'message': 'Продукт не найден'}), 404

        if quantity <= 0:
            return jsonify({'success': False, 'message': 'Количество должно быть больше 0'}), 400

        if quantity > product.number:
            return jsonify({
                'success': False,
                'message': f'Недостаточно товара на складе. В наличии: {product.number} шт.'
            }), 400

        # Инициализируем корзину в сессии
        if 'cart' not in session:
            session['cart'] = {}

        cart = session['cart']
        current_quantity = cart.get(product_name, 0)

        # Проверяем, не превышает ли общее количество доступное
        if current_quantity + quantity > product.number:
            return jsonify({
                'success': False,
                'message': f'Нельзя добавить больше {product.number} шт. Уже в корзине: {current_quantity} шт.'
            }), 400

        cart[product_name] = current_quantity + quantity
        session['cart'] = cart
        session.modified = True

        total_in_cart = sum(cart.values())

        return jsonify({
            'success': True,
            'message': f'Добавлено {quantity} {product_name} в корзину',
            'cart_total': total_in_cart,
            'product_in_cart': cart[product_name],
            'product_stock': product.number
        })

    except ValueError:
        return jsonify({'success': False, 'message': 'Некорректное количество'}), 400
    except Exception as e:
        return jsonify({'success': False, 'message': f'Ошибка сервера: {str(e)}'}), 500


@app.route('/cart')
def view_cart():
    """Страница корзины"""
    cart = session.get('cart', {})

    products_info = []
    total_cost = 0

    for product_name, quantity in cart.items():
        product = product_service.get_product(product_name)
        if product:
            products_info.append({
                'name': product.name,
                'quantity': quantity,
                'cost': product.cost,
                'total': product.cost * quantity,
                'emoji': product.emoji,
                'available': product.number
            })
            total_cost += product.cost * quantity

    return render_template('order_products.html',
                           products=products_info,
                           total_cost=total_cost,
                           cart=cart)


@app.route('/checkout', methods=['POST'])
def checkout():
    """Создание заказа и переход к оплате"""
    cart = session.get('cart', {})

    if not cart:
        flash('Корзина пуста', 'warning')
        return redirect(url_for('product_list'))

    # Проверяем наличие всех товаров
    unavailable_items = []
    for product_name, quantity in cart.items():
        product = product_service.get_product(product_name)
        if not product or product.number < quantity:
            unavailable_items.append(product_name)

    if unavailable_items:
        flash(f'Товары закончились: {", ".join(unavailable_items)}', 'error')
        return redirect(url_for('view_cart'))

    try:
        # Создаем заказ
        order = order_service.create_order(cart)

        # Сохраняем ID заказа в сессии
        session['current_order_id'] = order.id

        # Рассчитываем общую сумму
        order_total = 0
        for name, quantity in cart.items():
            product = product_service.get_product(name)
            if product:
                order_total += product.cost * quantity
        session['order_total'] = order_total

        flash('Заказ создан успешно! Переходим к оплате.', 'success')
        return redirect(url_for('payment_page'))

    except Exception as e:
        flash(f'Ошибка при создании заказа: {str(e)}', 'error')
        return redirect(url_for('view_cart'))


@app.route('/payment')
def payment_page():
    """Страница выбора способа оплаты"""
    order_id = session.get('current_order_id')
    total = session.get('order_total', 0)

    if not order_id:
        flash('Сначала создайте заказ', 'warning')
        return redirect(url_for('product_list'))

    # Проверяем существование заказа
    order = order_service.get_order(order_id)
    if not order or order.status != 'In progress':
        flash('Заказ не найден или уже обработан', 'error')
        return redirect(url_for('product_list'))

    return render_template('payment.html',
                           order_id=order_id,
                           total=total,
                           cart=session.get('cart', {}))


@app.route('/process_payment', methods=['POST'])
def process_payment():
    """Обработка платежа с тестовыми методами"""
    order_id = session.get('current_order_id')
    cart = session.get('cart', {})  # Получаем корзину ДО очистки сессии
    payment_method = request.form.get('payment_method', 'card')

    if not order_id:
        flash('Заказ не найден', 'error')
        return redirect(url_for('product_list'))

    if not cart:  # Проверяем, что корзина не пуста
        flash('Корзина пуста', 'error')
        return redirect(url_for('product_list'))

    # Проверяем тестовые методы
    if payment_method in ['reject', 'timeout']:
        # Это тестовый метод, обрабатываем соответственно
        if payment_method == 'reject':
            order_service.update_order_status(order_id, 'Rejected')
            flash('Платеж отклонен банком. Попробуйте другой способ оплаты.', 'error')
        elif payment_method == 'timeout':
            order_service.update_order_status(order_id, 'Rejected')
            flash('Время ожидания платежа истекло. Попробуйте еще раз.', 'error')
        return redirect(url_for('payment_page'))

    # Нормальные методы оплаты
    try:
        payment = payment_service.process_payment(order_id, payment_method)

        if payment.done:
            # Обновляем статус заказа
            order_service.update_order_status(order_id, 'Done')

            # Обновляем количество товаров на складе
            for product_name, quantity in cart.items():
                product_service.update_product_stock(product_name, quantity)

            # Сохраняем ID заказа для страницы успеха
            completed_order_id = order_id

            # ОЧИСТКА КОРЗИНЫ ПОСЛЕ УСПЕШНОЙ ОПЛАТЫ
            # 1. Очищаем сессию
            session.pop('cart', None)
            session.pop('current_order_id', None)
            session.pop('order_total', None)
            session.modified = True

            # 2. Устанавливаем флаг для очистки локального хранилища на клиенте
            session['payment_success'] = True
            session['cleared_order_id'] = completed_order_id

            flash('Платеж успешно обработан! Заказ оформлен.', 'success')
            return redirect(url_for('success_page', order_id=completed_order_id))
        else:
            order_service.update_order_status(order_id, 'Rejected')
            flash('Платеж не прошел. Попробуйте другой способ оплаты.', 'error')
            return redirect(url_for('payment_page'))

    except Exception as e:
        flash(f'Ошибка при обработке платежа: {str(e)}', 'error')
        return redirect(url_for('payment_page'))


@app.route('/clear_client_cart', methods=['POST'])
def clear_client_cart():
    """API для очистки корзины на клиенте после успешной оплаты"""
    try:
        # Проверяем, была ли успешная оплата
        if session.get('payment_success'):
            order_id = session.get('cleared_order_id')

            # Очищаем флаги
            session.pop('payment_success', None)
            session.pop('cleared_order_id', None)
            session.modified = True

            return jsonify({
                'success': True,
                'message': 'Корзина очищена после оплаты',
                'order_id': order_id,
                'cart': {}
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Нет данных об успешной оплате'
            })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/success')
def success_page():
    """Страница успешного оформления заказа"""
    order_id = request.args.get('order_id')

    if not order_id:
        # Если нет order_id в параметрах, попробуем получить из флеш-сообщений
        # или просто покажем общую страницу успеха
        return render_template('success.html', order_id='#неизвестен')

    # Получаем информацию о заказе из базы
    order = order_service.get_order(int(order_id))

    if order:
        return render_template('success.html', order_id=order_id)
    else:
        return render_template('success.html', order_id=f'#{order_id}')


@app.route('/clear_cart', methods=['POST'])
def clear_cart():
    """Очистить корзину"""
    try:
        session.pop('cart', None)
        session.modified = True
        return jsonify({'success': True, 'message': 'Корзина очищена'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/cart/item/<product_name>', methods=['GET'])
def get_cart_item(product_name):
    """API для получения информации о конкретном товаре в корзине"""
    cart = session.get('cart', {})
    quantity = cart.get(product_name, 0)

    product = product_service.get_product(product_name)
    if not product:
        return jsonify({'success': False, 'message': 'Товар не найден'}), 404

    item_total = product.cost * quantity

    return jsonify({
        'success': True,
        'product': {
            'name': product.name,
            'quantity': quantity,
            'cost': product.cost,
            'total': item_total,
            'available': product.number
        }
    })


@app.route('/update_cart_item', methods=['POST'])
def update_cart_item():
    """Обновить количество товара в корзине с возвратом детальной информации"""
    try:
        product_name = request.form.get('product_name', '').strip()
        quantity = int(request.form.get('quantity', 0))

        if not product_name:
            return jsonify({'success': False, 'message': 'Не указано название продукта'}), 400

        cart = session.get('cart', {})

        if not cart:
            cart = {}
            session['cart'] = cart

        product = product_service.get_product(product_name)
        if not product:
            return jsonify({'success': False, 'message': 'Товар не найден'}), 404

        if quantity < 0:
            return jsonify({'success': False, 'message': 'Количество не может быть отрицательным'}), 400

        if quantity > product.number:
            return jsonify({
                'success': False,
                'message': f'Недостаточно товара на складе. В наличии: {product.number} шт.'
            }), 400

        if quantity == 0:
            if product_name in cart:
                del cart[product_name]
            item_total = 0
        else:
            cart[product_name] = quantity
            item_total = product.cost * quantity

        session['cart'] = cart
        session.modified = True

        # Пересчитываем общую стоимость
        total_cost = 0
        items_details = []
        for name, qty in cart.items():
            prod = product_service.get_product(name)
            if prod:
                item_cost = prod.cost * qty
                total_cost += item_cost
                items_details.append({
                    'name': name,
                    'quantity': qty,
                    'cost': prod.cost,
                    'total': item_cost
                })

        return jsonify({
            'success': True,
            'cart_total': sum(cart.values()),
            'total_cost': total_cost,
            'item_total': item_total,
            'remaining_stock': product.number,
            'items': items_details,
            'cart': cart  # Возвращаем обновленную корзину
        })

    except ValueError:
        return jsonify({'success': False, 'message': 'Некорректное количество'}), 400
    except Exception as e:
        return jsonify({'success': False, 'message': f'Ошибка сервера: {str(e)}'}), 500


@app.route('/remove_from_cart/<product_name>', methods=['POST'])
def remove_from_cart(product_name):
    """Удалить товар из корзины"""
    cart = session.get('cart', {})

    if product_name in cart:
        del cart[product_name]
        session['cart'] = cart
        session.modified = True
        flash(f'{product_name} удален из корзины', 'success')

    return redirect(url_for('view_cart'))


@app.route('/api/sync_cart', methods=['POST'])
def sync_cart():
    """Синхронизация корзины с клиентского хранилища"""
    try:
        data = request.get_json()
        client_cart = data.get('cart', {})

        if not isinstance(client_cart, dict):
            return jsonify({'success': False, 'message': 'Некорректный формат корзины'}), 400

        # Валидируем корзину
        validated_cart = {}
        for product_name, quantity in client_cart.items():
            product = product_service.get_product(product_name)
            if product and isinstance(quantity, int) and quantity > 0:
                validated_cart[product_name] = min(quantity, product.number)

        session['cart'] = validated_cart
        session.modified = True

        return jsonify({
            'success': True,
            'message': 'Корзина синхронизирована',
            'cart': validated_cart
        })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/cart')
def get_cart_api():
    """API для получения текущей корзины"""
    cart = session.get('cart', {})
    cart_details = []
    total = 0

    for product_name, quantity in cart.items():
        product = product_service.get_product(product_name)
        if product:
            item_total = product.cost * quantity
            cart_details.append({
                'name': product.name,
                'quantity': quantity,
                'cost': product.cost,
                'total': item_total,
                'emoji': product.emoji,
                'available': product.number
            })
            total += item_total

    return jsonify({
        'success': True,
        'cart': cart,
        'items': cart_details,
        'total': total,
        'count': sum(cart.values())
    })


@app.errorhandler(404)
def not_found_error(error):
    """Обработка 404 ошибки"""
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_error(error):
    """Обработка 500 ошибки"""
    return render_template('500.html'), 500


@app.route('/sync_cart_state', methods=['POST'])
def sync_cart_state():
    """Синхронизация состояния корзины между клиентом и сервером"""
    try:
        data = request.get_json()
        client_cart = data.get('cart', {})

        if not isinstance(client_cart, dict):
            return jsonify({'success': False, 'message': 'Некорректный формат корзины'}), 400

        # Получаем текущую корзину из сессии
        server_cart = session.get('cart', {})

        # Синхронизируем: берем максимальные значения между клиентом и сервером
        synchronized_cart = {}
        all_product_names = set(list(client_cart.keys()) + list(server_cart.keys()))

        for product_name in all_product_names:
            client_qty = client_cart.get(product_name, 0)
            server_qty = server_cart.get(product_name, 0)

            # Берем максимальное значение для избежания потерь
            synchronized_cart[product_name] = max(client_qty, server_qty)

        # Валидируем синхронизированную корзину
        validated_cart = {}
        for product_name, quantity in synchronized_cart.items():
            product = product_service.get_product(product_name)
            if product and isinstance(quantity, int) and quantity > 0:
                validated_cart[product_name] = min(quantity, product.number)

        # Сохраняем валидированную корзину
        session['cart'] = validated_cart
        session.modified = True

        return jsonify({
            'success': True,
            'message': 'Состояние корзины синхронизировано',
            'cart': validated_cart,
            'cart_total': sum(validated_cart.values())
        })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

if __name__ == '__main__':
    # Создаем необходимые папки
    templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
    static_dir = os.path.join(os.path.dirname(__file__), 'static')
    css_dir = os.path.join(static_dir, 'css')
    js_dir = os.path.join(static_dir, 'js')
    icons_dir = os.path.join(static_dir, 'icons')

    os.makedirs(templates_dir, exist_ok=True)
    os.makedirs(css_dir, exist_ok=True)
    os.makedirs(js_dir, exist_ok=True)
    os.makedirs(icons_dir, exist_ok=True)

    # Создаем простые иконки для тем (можно заменить на реальные SVG)
    sun_icon = '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
        <path d="M12 2.25a.75.75 0 01.75.75v2.25a.75.75 0 01-1.5 0V3a.75.75 0 01.75-.75zM7.5 12a4.5 4.5 0 119 0 4.5 4.5 0 01-9 0zM18.894 6.166a.75.75 0 00-1.06-1.06l-1.591 1.59a.75.75 0 101.06 1.061l1.591-1.59zM21.75 12a.75.75 0 01-.75.75h-2.25a.75.75 0 010-1.5H21a.75.75 0 01.75.75zM17.834 18.894a.75.75 0 001.06-1.06l-1.59-1.591a.75.75 0 10-1.061 1.06l1.59 1.591zM12 18a.75.75 0 01.75.75V21a.75.75 0 01-1.5 0v-2.25A.75.75 0 0112 18zM7.758 17.303a.75.75 0 00-1.061-1.06l-1.591 1.59a.75.75 0 001.06 1.061l1.591-1.59zM6 12a.75.75 0 01-.75.75H3a.75.75 0 010-1.5h2.25A.75.75 0 016 12zM6.697 7.757a.75.75 0 001.06-1.06l-1.59-1.591a.75.75 0 00-1.061 1.06l1.59 1.591z"/>
    </svg>'''

    moon_icon = '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
        <path fill-rule="evenodd" d="M9.528 1.718a.75.75 0 01.162.819A8.97 8.97 0 009 6a9 9 0 009 9 8.97 8.97 0 003.463-.69.75.75 0 01.981.98 10.503 10.503 0 01-9.694 6.46c-5.799 0-10.5-4.701-10.5-10.5 0-4.368 2.667-8.112 6.46-9.694a.75.75 0 01.818.162z" clip-rule="evenodd"/>
    </svg>'''

    with open(os.path.join(icons_dir, 'sun.svg'), 'w') as f:
        f.write(sun_icon)
    with open(os.path.join(icons_dir, 'moon.svg'), 'w') as f:
        f.write(moon_icon)

    app.run(host='0.0.0.0', port=5000, debug=True)
