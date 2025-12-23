class CartManager {
    constructor() {
        this.syncInProgress = false;
        this.pendingSync = null;
        this.inputTimeout = null;
        this.buttonClickTimeout = null;
        this.init();
        console.log('CartManager initialized');
    }

    init() {
        this.loadCartFromStorage();
        this.updateCartIndicator();
        this.setupEventListeners();

        // Синхронизация при загрузке страницы
        setTimeout(() => this.syncCartWithServer(), 1000);
    }

    // Обновить итоговую сумму на странице корзины
    updateCartTotal(total) {
        console.log('Updating cart total to:', total);
        
        // Обновляем основной элемент с суммой
        const totalElement = document.getElementById('cart-total-amount');
        if (totalElement) {
            const currentTotal = parseFloat(totalElement.textContent.replace('₽', '').replace(/\s/g, '').replace(',', '.')) || 0;
            totalElement.textContent = `${total} ₽`;

            // Анимация изменения суммы
            if (parseFloat(total) !== currentTotal) {
                totalElement.classList.add('updated');
                setTimeout(() => {
                    totalElement.classList.remove('updated');
                }, 500);
            }
        }

        // Также обновляем другие элементы с суммой если они есть
        document.querySelectorAll('.cart-total').forEach(element => {
            element.textContent = `${total} ₽`;
        });
    }

    // Обновить количество позиций
    updateItemCount(count) {
        const countElement = document.getElementById('cart-total-count');
        if (countElement) {
            const currentText = countElement.textContent;
            // Заменяем только число позиций, сохраняя остальной текст
            countElement.textContent = currentText.replace(/Всего товаров: \d+/, `Всего товаров: ${count}`);
        }
    }

    async loadCartFromStorage() {
        const savedCart = localStorage.getItem('fruitShopCart');
        if (savedCart) {
            try {
                const cart = JSON.parse(savedCart);
                if (cart && typeof cart === 'object') {
                    // Отправляем данные на сервер для синхронизации
                    await this.syncCartState(cart);
                }
            } catch (e) {
                console.error('Failed to load cart from storage:', e);
            }
        }
    }

    async syncCartState(clientCart) {
        // Если уже идет синхронизация, откладываем запрос
        if (this.syncInProgress) {
            this.pendingSync = clientCart;
            return;
        }

        this.syncInProgress = true;

        try {
            const response = await fetch('/sync_cart_state', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ cart: clientCart })
            });

            const data = await response.json();

            if (data.success) {
                // Обновляем локальное хранилище синхронизированной корзиной
                localStorage.setItem('fruitShopCart', JSON.stringify(data.cart));

                // Обновляем глобальную переменную
                window.cartData = data.cart;

                // Обновляем индикатор
                this.updateCartIndicator(data.cart_total);

                // Обновляем страницу если нужно
                if (window.location.pathname.includes('/cart')) {
                    this.refreshCartPage();
                }
            }
        } catch (error) {
            console.error('Failed to sync cart state:', error);
        } finally {
            this.syncInProgress = false;

            // Обрабатываем отложенный запрос
            if (this.pendingSync) {
                const pending = this.pendingSync;
                this.pendingSync = null;
                await this.syncCartState(pending);
            }
        }
    }

    async syncCartWithServer() {
        // Получаем текущее состояние корзины с сервера
        try {
            const response = await fetch('/api/cart');
            const data = await response.json();

            if (data.success) {
                // Обновляем локальное хранилище
                localStorage.setItem('fruitShopCart', JSON.stringify(data.cart));
                window.cartData = data.cart;
                this.updateCartIndicator(data.count);

                // Если на странице корзины - обновляем отображение
                if (window.location.pathname.includes('/cart')) {
                    this.refreshCartPage();
                }
            }
        } catch (error) {
            console.error('Failed to sync with server:', error);
        }
    }

    async addToCart(productName, quantity) {
        if (quantity <= 0) {
            throw new Error('Количество должно быть больше 0');
        }

        try {
            const response = await fetch('/add_to_cart', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                },
                body: `product_name=${encodeURIComponent(productName)}&quantity=${quantity}`
            });

            const data = await response.json();

            if (!data.success) {
                throw new Error(data.message);
            }

            // Обновляем локальное хранилище
            const currentCart = JSON.parse(localStorage.getItem('fruitShopCart') || '{}');
            currentCart[productName] = (currentCart[productName] || 0) + quantity;
            localStorage.setItem('fruitShopCart', JSON.stringify(currentCart));
            window.cartData = currentCart;

            this.updateCartIndicator(data.cart_total);
            this.showNotification(data.message, 'success');

            return data;

        } catch (error) {
            this.showNotification(error.message, 'error');
            throw error;
        }
    }

    async updateCartItem(productName, quantity) {
        if (quantity < 0) return;
        
        try {
            const response = await fetch('/update_cart_item', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                },
                body: `product_name=${encodeURIComponent(productName)}&quantity=${quantity}`
            });

            const data = await response.json();

            if (data.success) {
                // Обновляем локальное хранилище
                const currentCart = JSON.parse(localStorage.getItem('fruitShopCart') || '{}');

                if (quantity === 0) {
                    delete currentCart[productName];
                } else {
                    currentCart[productName] = quantity;
                }

                localStorage.setItem('fruitShopCart', JSON.stringify(currentCart));
                window.cartData = currentCart;

                // Обновляем UI если на странице корзины
                if (window.location.pathname.includes('/cart')) {
                    if (quantity === 0) {
                        this.removeCartItemFromUI(productName);
                    } else {
                        this.updateCartItemInUI(productName, quantity, data);
                    }

                    // ВАЖНО: Обновляем итоговую сумму
                    if (data.total_cost !== undefined) {
                        this.updateCartTotal(data.total_cost);
                    }
                    
                    // Обновляем количество позиций
                    this.updateItemCount(data.cart_total || this.sumCartItems(currentCart));

                    // Если корзина пуста, перезагружаем страницу
                    if (Object.keys(currentCart).length === 0) {
                        setTimeout(() => {
                            window.location.reload();
                        }, 500);
                    }
                }

                // Обновляем индикатор
                this.updateCartIndicator(data.cart_total || this.sumCartItems(currentCart));

                // Добавляем уведомление
                this.showNotification('Корзина обновлена', 'success');

                return data;
            }
        } catch (error) {
            console.error('Failed to update cart item:', error);
            this.showNotification('Ошибка при обновлении количества', 'error');
        }
    }

    sumCartItems(cart) {
        return Object.values(cart).reduce((sum, quantity) => sum + quantity, 0);
    }

    removeCartItemFromUI(productName) {
        const row = document.querySelector(`[data-product="${productName}"]`);
        if (row) {
            row.style.transform = 'translateX(100%)';
            row.style.opacity = '0';
            setTimeout(() => {
                if (row.parentNode) {
                    row.parentNode.removeChild(row);
                }
            }, 300);
        }
    }

    updateCartItemInUI(productName, quantity, data) {
        const row = document.querySelector(`[data-product="${productName}"]`);
        if (row) {
            // Обновляем input значение
            const input = row.querySelector('.cart-quantity-input');
            if (input) {
                input.value = quantity;
            }

            // Обновляем сумму для конкретного товара
            let itemTotal = 0;
            if (data.items && Array.isArray(data.items)) {
                const itemData = data.items.find(item => item.name === productName);
                if (itemData) {
                    itemTotal = itemData.total;
                }
            }
            
            // Если не нашли в items, вычисляем
            if (itemTotal === 0) {
                const priceElement = row.querySelector('.item-price');
                if (priceElement) {
                    const price = parseInt(priceElement.textContent.replace('₽', '').replace(/\s/g, '')) || 0;
                    itemTotal = price * quantity;
                }
            }

            const totalCell = row.querySelector('.item-total');
            if (totalCell) {
                totalCell.textContent = `${itemTotal} ₽`;
                totalCell.classList.add('updated');
                setTimeout(() => totalCell.classList.remove('updated'), 500);
            }

            // Обновляем состояние кнопок
            this.updateButtonStates(productName, quantity, row);

            // Анимация строки
            row.style.backgroundColor = 'rgba(16, 185, 129, 0.1)';
            setTimeout(() => {
                row.style.backgroundColor = '';
            }, 500);
        }
    }

    async clearCart() {
        if (!confirm('Вы уверены, что хотите очистить корзину?')) {
            return;
        }

        try {
            const response = await fetch('/clear_cart', {
                method: 'POST'
            });

            const data = await response.json();

            if (data.success) {
                // Очищаем локальное хранилище
                localStorage.removeItem('fruitShopCart');
                window.cartData = {};

                this.updateCartIndicator(0);
                this.updateCartTotal(0);

                this.showNotification('Корзина очищена', 'success');

                // Если на странице корзины - перезагружаем
                if (window.location.pathname.includes('/cart')) {
                    // Анимация очистки
                    const rows = document.querySelectorAll('[data-product]');
                    rows.forEach((row, index) => {
                        setTimeout(() => {
                            row.style.transform = 'translateX(100%)';
                            row.style.opacity = '0';
                        }, index * 100);
                    });

                    // Перезагрузка через секунду
                    setTimeout(() => {
                        window.location.reload();
                    }, 1000);
                }
            }
        } catch (error) {
            console.error('Failed to clear cart:', error);
            this.showNotification('Ошибка при очистке корзины', 'error');
        }
    }

    refreshCartPage() {
        // Проверяем, нужно ли обновить страницу
        const currentProducts = document.querySelectorAll('[data-product]').length;
        const serverProductCount = Object.keys(window.cartData || {}).length;

        if (currentProducts !== serverProductCount) {
            // Если количество товаров отличается, перезагружаем страницу
            setTimeout(() => {
                window.location.reload();
            }, 100);
        }
    }

    setupEventListeners() {
        // Единый обработчик всех кликов
        document.addEventListener('click', (e) => {
            const target = e.target;
            
            // 1. Кнопки + и - на странице продуктов (главная страница)
            if (target.classList.contains('quantity-btn')) {
                e.preventDefault();
                e.stopPropagation();
                
                const isPlus = target.classList.contains('plus') || target.textContent.includes('+');
                const isMinus = target.classList.contains('minus') || target.textContent.includes('-');
                
                if (isPlus || isMinus) {
                    // Находим соответствующий input
                    let input;
                    if (isPlus) {
                        input = target.previousElementSibling;
                    } else {
                        input = target.nextElementSibling;
                    }
                    
                    if (input && input.classList.contains('quantity-input')) {
                        const productName = input.id.replace('quantity-', '');
                        this.handleQuantityButtonClick(productName, isPlus ? 1 : -1, input);
                    }
                }
                return;
            }
            
            // 2. Кнопки добавления в корзину на главной странице
            if (target.classList.contains('add-to-cart-btn') || target.closest('.add-to-cart-btn')) {
                e.preventDefault();
                e.stopPropagation();
                const button = target.classList.contains('add-to-cart-btn') ? target : target.closest('.add-to-cart-btn');
                this.addToCartFromButton(button);
                return;
            }
            
            // 3. Кнопки + в корзине
            if (target.classList.contains('cart-increase') || target.closest('.cart-increase')) {
                e.preventDefault();
                e.stopPropagation();
                const button = target.classList.contains('cart-increase') ? target : target.closest('.cart-increase');
                const productName = button.dataset.productName;
                this.handleCartIncrease(productName, button);
                return;
            }
            
            // 4. Кнопки - в корзине
            if (target.classList.contains('cart-decrease') || target.closest('.cart-decrease')) {
                e.preventDefault();
                e.stopPropagation();
                const button = target.classList.contains('cart-decrease') ? target : target.closest('.cart-decrease');
                const productName = button.dataset.productName;
                this.handleCartDecrease(productName, button);
                return;
            }
            
            // 5. Кнопки удаления из корзины
            if (target.classList.contains('remove-from-cart') || target.closest('.remove-from-cart')) {
                e.preventDefault();
                e.stopPropagation();
                const button = target.classList.contains('remove-from-cart') ? target : target.closest('.remove-from-cart');
                const productName = button.dataset.productName;
                this.removeFromCart(productName);
                return;
            }
            
            // 6. Кнопка очистки корзины
            if (target.classList.contains('clear-cart-btn') || target.closest('.clear-cart-btn')) {
                e.preventDefault();
                e.stopPropagation();
                this.clearCart();
                return;
            }
        });

        // Обработка изменений через input в корзине
        document.addEventListener('input', (e) => {
            if (e.target.classList.contains('cart-quantity-input')) {
                const productName = e.target.dataset.productName;
                const quantity = parseInt(e.target.value) || 0;

                // Дебаунс для избежания множественных запросов
                clearTimeout(this.inputTimeout);
                this.inputTimeout = setTimeout(() => {
                    this.updateCartItem(productName, quantity);
                }, 500);
            }
        });

        // Обработка изменений через input на главной странице
        document.addEventListener('change', (e) => {
            if (e.target.classList.contains('quantity-input')) {
                const input = e.target;
                const max = parseInt(input.max) || 100;
                const min = parseInt(input.min) || 1;
                let value = parseInt(input.value) || min;

                if (value > max) {
                    value = max;
                    this.showNotification(`Максимальное количество: ${max}`, 'warning');
                } else if (value < min) {
                    value = min;
                }

                if (value !== parseInt(input.value)) {
                    input.value = value;
                }
            }
        });
    }

    // Обработка кнопок +/- на главной странице
    handleQuantityButtonClick(productName, change, input) {
        const max = parseInt(input.max) || 100;
        const min = parseInt(input.min) || 1;
        let currentValue = parseInt(input.value) || min;
        const newValue = Math.max(min, Math.min(max, currentValue + change));

        if (newValue !== currentValue) {
            input.value = newValue;
            
            // Анимация кнопки
            const button = change > 0 ? input.nextElementSibling : input.previousElementSibling;
            if (button) {
                button.classList.add('animate-press');
                setTimeout(() => button.classList.remove('animate-press'), 300);
            }
            
            // Обновляем UI
            const event = new Event('change', { bubbles: true });
            input.dispatchEvent(event);
        }
    }

    // Обработка кнопки + в корзине
    handleCartIncrease(productName, button) {
        const input = document.querySelector(`input[data-product-name="${productName}"]`);
        if (!input) return;

        const max = parseInt(input.max) || 100;
        let currentValue = parseInt(input.value) || 0;
        const newValue = Math.max(1, Math.min(max, currentValue + 1));

        if (newValue !== currentValue) {
            this.updateCartItemDirect(productName, newValue, button);
        }
    }

    // Обработка кнопки - в корзине
    handleCartDecrease(productName, button) {
        const input = document.querySelector(`input[data-product-name="${productName}"]`);
        if (!input) return;

        const min = parseInt(input.min) || 1;
        let currentValue = parseInt(input.value) || 0;
        const newValue = Math.max(min, currentValue - 1);

        if (newValue !== currentValue) {
            this.updateCartItemDirect(productName, newValue, button);
        }
    }

    // Прямое обновление товара в корзине
    async updateCartItemDirect(productName, quantity, button) {
        // Анимация кнопки
        button.classList.add('animate-press');
        setTimeout(() => button.classList.remove('animate-press'), 300);

        // Временно показываем обновленную сумму
        this.updateItemPricePreview(productName, quantity);

        // Обновляем через API
        try {
            const data = await this.updateCartItem(productName, quantity);
            
            if (data && data.success) {
                // Обновляем итоговую сумму
                if (data.total_cost !== undefined) {
                    this.updateCartTotal(data.total_cost);
                }
            }
        } catch (error) {
            console.error('Error updating cart item:', error);
            this.showNotification('Ошибка при обновлении', 'error');
        }
    }

    async addToCartFromButton(button) {
        const productName = button.dataset.productName;
        const quantityInput = document.getElementById(`quantity-${productName}`);
        const quantity = quantityInput ? parseInt(quantityInput.value) || 1 : 1;

        // Сохраняем оригинальный текст кнопки
        const originalText = button.innerHTML;

        // Показываем загрузку
        button.disabled = true;
        button.innerHTML = '<span class="loader"></span>';

        try {
            await this.addToCart(productName, quantity);
            this.showNotification(`Добавлено ${quantity} ${productName} в корзину`, 'success');

            // Анимация успешного добавления
            button.innerHTML = '<i class="fas fa-check"></i>';
            button.style.background = 'linear-gradient(135deg, #10b981, #059669)';

            setTimeout(() => {
                button.disabled = false;
                button.innerHTML = originalText;
                button.style.background = '';

                // Сброс количества в инпуте
                if (quantityInput) {
                    quantityInput.value = 1;
                }
            }, 1000);

        } catch (error) {
            this.showNotification(error.message, 'error');
            button.disabled = false;
            button.innerHTML = originalText;
        }
    }

    updateButtonStates(productName, quantity, row) {
        if (!row) {
            row = document.querySelector(`[data-product="${productName}"]`);
            if (!row) return;
        }

        const input = row.querySelector('.cart-quantity-input');
        const decreaseBtn = row.querySelector('.cart-decrease');
        const increaseBtn = row.querySelector('.cart-increase');

        if (input && decreaseBtn && increaseBtn) {
            const max = parseInt(input.max) || 100;

            // Обновляем состояние кнопок
            decreaseBtn.disabled = quantity <= 1;
            increaseBtn.disabled = quantity >= max;

            // Визуальная обратная связь
            if (decreaseBtn.disabled) {
                decreaseBtn.style.opacity = '0.5';
                decreaseBtn.style.cursor = 'not-allowed';
            } else {
                decreaseBtn.style.opacity = '1';
                decreaseBtn.style.cursor = 'pointer';
            }

            if (increaseBtn.disabled) {
                increaseBtn.style.opacity = '0.5';
                increaseBtn.style.cursor = 'not-allowed';
            } else {
                increaseBtn.style.opacity = '1';
                increaseBtn.style.cursor = 'pointer';
            }
        }
    }

    updateCartIndicator(total) {
        const cartIndicator = document.querySelector('.cart-indicator');
        if (cartIndicator) {
            if (total > 0) {
                cartIndicator.innerHTML = '🍹';
                cartIndicator.classList.remove('empty');

                // Анимация при добавлении товара
                if (!cartIndicator.classList.contains('bounce')) {
                    cartIndicator.classList.add('bounce');
                    setTimeout(() => {
                        cartIndicator.classList.remove('bounce');
                    }, 1000);
                }
            } else {
                cartIndicator.innerHTML = '🍹';
                cartIndicator.classList.add('empty');
            }
        }
    }

    saveCartToStorage() {
        try {
            localStorage.setItem('fruitShopCart', JSON.stringify(window.cartData || {}));
        } catch (e) {
            console.error('Failed to save cart to storage:', e);
        }
    }

    showNotification(message, type = 'info') {
        // Создаем элемент уведомления
        const notification = document.createElement('div');
        notification.className = `message ${type} fixed-notification`;
        notification.innerHTML = `
            <i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'exclamation-circle' : 'info-circle'} mr-2"></i>
            ${message}
        `;
        notification.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            z-index: 1000;
            animation: slideIn 0.3s ease-out;
            max-width: 300px;
            box-shadow: 0 10px 25px rgba(0,0,0,0.1);
        `;

        document.body.appendChild(notification);

        // Удаление уведомления через 3 секунды
        setTimeout(() => {
            notification.style.animation = 'slideOut 0.3s ease-out';
            setTimeout(() => {
                if (notification.parentNode) {
                    notification.parentNode.removeChild(notification);
                }
            }, 300);
        }, 3000);
    }

    // Предпросмотр цены при изменении количества
    updateItemPricePreview(productName, quantity) {
        const row = document.querySelector(`[data-product="${productName}"]`);
        if (row) {
            const priceElement = row.querySelector('.item-price');
            const totalElement = row.querySelector('.item-total');
            
            if (priceElement && totalElement) {
                const price = parseInt(priceElement.textContent.replace('₽', '').replace(/\s/g, '')) || 0;
                const newTotal = price * quantity;
                
                // Временно показываем новую сумму
                totalElement.textContent = `${newTotal} ₽`;
                totalElement.style.color = '#10b981';
                totalElement.style.fontWeight = 'bold';
                
                // Через секунду возвращаем нормальный вид
                setTimeout(() => {
                    if (totalElement.textContent === `${newTotal} ₽`) {
                        totalElement.style.color = '';
                        totalElement.style.fontWeight = '';
                    }
                }, 300);
            }
        }
    }
}

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    window.cartManager = new CartManager();

    // Инициализируем данные корзины если они есть
    if (window.cartData) {
        window.cartManager.updateCartIndicator(Object.values(window.cartData).reduce((a, b) => a + b, 0));
    }

    // Инициализируем все инпуты количества
    document.querySelectorAll('.quantity-input').forEach(input => {
        input.addEventListener('change', function() {
            const max = parseInt(this.max);
            const value = parseInt(this.value);
            if (value > max) {
                this.value = max;
                window.cartManager.showNotification(`Максимальное количество: ${max}`, 'warning');
            }
            if (value < 1) {
                this.value = 1;
            }
        });
    });

    // Добавляем CSS анимации
    const style = document.createElement('style');
    style.textContent = `
        @keyframes slideIn {
            from {
                transform: translateX(100%);
                opacity: 0;
            }
            to {
                transform: translateX(0);
                opacity: 1;
            }
        }

        @keyframes slideOut {
            from {
                transform: translateX(0);
                opacity: 1;
            }
            to {
                transform: translateX(100%);
                opacity: 0;
            }
        }

        @keyframes bounce {
            0%, 100% { transform: scale(1); }
            50% { transform: scale(1.2); }
        }

        .bounce {
            animation: bounce 0.5s ease;
        }

        @keyframes press {
            0% { transform: scale(1); }
            50% { transform: scale(0.9); }
            100% { transform: scale(1); }
        }

        .animate-press {
            animation: press 0.3s ease;
        }

        @keyframes price-update {
            0% { transform: scale(1); color: inherit; }
            50% { transform: scale(1.1); color: #10b981; }
            100% { transform: scale(1); color: inherit; }
        }

        .updated {
            animation: price-update 0.5s ease;
        }

        .loader {
            display: inline-block;
            width: 16px;
            height: 16px;
            border: 2px solid #f3f3f3;
            border-radius: 50%;
            border-top-color: #3498db;
            animation: spin 1s linear infinite;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .fixed-notification {
            position: fixed !important;
            top: 20px !important;
            right: 20px !important;
            z-index: 10000 !important;
        }
    `;
    document.head.appendChild(style);
});

// Полифил для старых браузеров
if (!Element.prototype.matches) {
    Element.prototype.matches =
        Element.prototype.matchesSelector ||
        Element.prototype.mozMatchesSelector ||
        Element.prototype.msMatchesSelector ||
        Element.prototype.oMatchesSelector ||
        Element.prototype.webkitMatchesSelector ||
        function(s) {
            var matches = (this.document || this.ownerDocument).querySelectorAll(s),
                i = matches.length;
            while (--i >= 0 && matches.item(i) !== this) {}
            return i > -1;
        };
}

if (!Element.prototype.closest) {
    Element.prototype.closest = function(s) {
        var el = this;
        if (!document.documentElement.contains(el)) return null;
        do {
            if (el.matches(s)) return el;
            el = el.parentElement || el.parentNode;
        } while (el !== null && el.nodeType === 1);
        return null;
    };
}

// Для отладки - логируем все клики по кнопкам
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('cart-increase') || 
        e.target.classList.contains('cart-decrease') ||
        e.target.classList.contains('quantity-btn')) {
        console.log('Button clicked:', e.target.className, e.target);
    }
}, true);