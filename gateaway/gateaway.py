from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import os

app = Flask(__name__,
            template_folder=os.path.join(os.path.dirname(__file__), 'templates'),
            static_folder=os.path.join(os.path.dirname(__file__), 'static'))
app.secret_key = 'your-secret-key-here'

# Импортируем сервисы
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from Product.ProductService import product_service
from Order.OrderService import order_service
from Payment.PaymentService import payment_service


# Контекстный процессор для передачи данных во все шаблоны
@app.context_processor
def inject_cart_data():
    def get_cart_total():
        cart = session.get('cart', {})
        return sum(cart.values()) if cart else 0

    def get_cart_items():
        return session.get('cart', {})

    def get_cart_count():
        cart = session.get('cart', {})
        return sum(cart.values()) if cart else 0

    return {
        'cart_total': get_cart_total,
        'cart_items': get_cart_items,
        'cart_count': get_cart_count
    }


@app.route('/')
def product_list():
    """Страница списка продуктов"""
    products = product_service.get_all_products()
    return render_template('product_list.html', products=products)


@app.route('/add_to_cart', methods=['POST'])
def add_to_cart():
    """Добавить продукт в корзину"""
    product_name = request.form.get('product_name')
    quantity = int(request.form.get('quantity', 1))

    product = product_service.get_product(product_name)
    if not product:
        return jsonify({'success': False, 'message': 'Продукт не найден'})

    if quantity > product.number:
        return jsonify({'success': False, 'message': f'Недостаточно товара на складе. В наличии: {product.number} шт.'})

    if quantity <= 0:
        return jsonify({'success': False, 'message': 'Количество должно быть больше 0'})

    # Инициализируем корзину в сессии
    if 'cart' not in session:
        session['cart'] = {}

    # Добавляем товар в корзину
    cart = session['cart']
    current_quantity = cart.get(product_name, 0)

    # Проверяем, не превышает ли общее количество доступное
    if current_quantity + quantity > product.number:
        return jsonify({
            'success': False,
            'message': f'Нельзя добавить больше {product.number} шт. Уже в корзине: {current_quantity} шт.'
        })

    cart[product_name] = current_quantity + quantity
    session['cart'] = cart
    session.modified = True

    total_in_cart = sum(cart.values())

    return jsonify({
        'success': True,
        'message': f'Добавлено {quantity} {product_name} в корзину',
        'cart_total': total_in_cart,
        'product_in_cart': cart[product_name]
    })


@app.route('/cart')
def view_cart():
    """Страница корзины (список продуктов в заказе)"""
    cart = session.get('cart', {})

    # Получаем информацию о продуктах
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
                'emoji': product.emoji
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
        return redirect(url_for('product_list'))

    # Проверяем наличие всех товаров
    for product_name, quantity in cart.items():
        product = product_service.get_product(product_name)
        if not product or product.number < quantity:
            return render_template('order_products.html',
                                   products=[],
                                   total_cost=0,
                                   error=f"Товар '{product_name}' закончился или его количество изменилось")

    # Создаем заказ
    order = order_service.create_order(cart)

    # Сохраняем ID заказа в сессии
    session['current_order_id'] = order.id
    session['order_total'] = sum(
        product_service.get_product(name).cost * quantity
        for name, quantity in cart.items()
    )

    return redirect(url_for('payment_page'))


@app.route('/payment')
def payment_page():
    """Страница выбора способа оплаты"""
    order_id = session.get('current_order_id')
    total = session.get('order_total', 0)

    if not order_id:
        return redirect(url_for('product_list'))

    return render_template('payment.html', order_id=order_id, total=total)


@app.route('/process_payment', methods=['POST'])
def process_payment():
    """Обработка платежа"""
    order_id = session.get('current_order_id')
    payment_method = request.form.get('payment_method')

    if not order_id:
        return redirect(url_for('product_list'))

    # Обрабатываем платеж
    payment = payment_service.process_payment(order_id, payment_method)

    if payment.done:
        # Обновляем статус заказа
        order_service.update_order_status(order_id, 'Done')

        # Обновляем количество товаров на складе
        cart = session.get('cart', {})
        for product_name, quantity in cart.items():
            product_service.update_product_stock(product_name, quantity)

        # Очищаем корзину и текущий заказ
        session.pop('cart', None)
        session.pop('current_order_id', None)
        session.pop('order_total', None)

        return redirect(url_for('success_page'))
    else:
        # Если платеж не прошел, отменяем заказ
        order_service.update_order_status(order_id, 'Rejected')
        return render_template('payment.html',
                               order_id=order_id,
                               total=session.get('order_total', 0),
                               error="Платеж не прошел. Попробуйте еще раз или выберите другой способ оплаты.")


@app.route('/success')
def success_page():
    """Страница успешного оформления заказа"""
    return render_template('success.html')


@app.route('/clear_cart', methods=['POST'])
def clear_cart():
    """Очистить корзину"""
    session.pop('cart', None)
    session.modified = True
    return jsonify({'success': True, 'message': 'Корзина очищена'})


@app.route('/update_cart_item', methods=['POST'])
def update_cart_item():
    """Обновить количество товара в корзине"""
    product_name = request.form.get('product_name')
    quantity = int(request.form.get('quantity', 0))

    cart = session.get('cart', {})

    if not cart or product_name not in cart:
        return jsonify({'success': False, 'message': 'Товар не найден в корзине'})

    product = product_service.get_product(product_name)
    if not product:
        return jsonify({'success': False, 'message': 'Товар не найден'})

    if quantity <= 0:
        # Удаляем товар из корзины
        del cart[product_name]
    else:
        if quantity > product.number:
            return jsonify(
                {'success': False, 'message': f'Недостаточно товара на складе. В наличии: {product.number} шт.'})
        cart[product_name] = quantity

    session['cart'] = cart
    session.modified = True

    # Пересчитываем общую стоимость
    total_cost = 0
    for name, qty in cart.items():
        prod = product_service.get_product(name)
        if prod:
            total_cost += prod.cost * qty

    return jsonify({
        'success': True,
        'cart_total': sum(cart.values()),
        'total_cost': total_cost
    })


@app.route('/remove_from_cart/<product_name>', methods=['POST'])
def remove_from_cart(product_name):
    """Удалить товар из корзины"""
    cart = session.get('cart', {})

    if product_name in cart:
        del cart[product_name]
        session['cart'] = cart
        session.modified = True

    return redirect(url_for('view_cart'))


if __name__ == '__main__':
    # Создаем папки для шаблонов
    templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
    os.makedirs(templates_dir, exist_ok=True)

    app.run(host='0.0.0.0', port=5000, debug=True)