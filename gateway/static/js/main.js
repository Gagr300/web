class CartManager {
    constructor() {
        this.syncInProgress = false;
        this.pendingSync = null;
        this.init();
    }

    init() {
        this.loadCartFromStorage();
        this.updateCartIndicator();
        this.setupEventListeners();

        // Синхронизация при загрузке страницы
        setTimeout(() => this.syncCartWithServer(), 1000);
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

                    // Обновляем итоговую сумму
                    this.updateCartTotal(data.total_cost);

                    // Если корзина пуста, перезагружаем страницу
                    if (Object.keys(currentCart).length === 0) {
                        setTimeout(() => {
                            window.location.reload();
                        }, 500);
                    }
                }

                // Обновляем индикатор
                this.updateCartIndicator(data.cart_total);

                this.showNotification('Корзина обновлена', 'success');
            }
        } catch (error) {
            console.error('Failed to update cart item:', error);
            this.showNotification('Ошибка при обновлении количества', 'error');
        }
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
            const itemData = data.items.find(item => item.name === productName);
            if (itemData) {
                const totalCell = row.querySelector('.item-total');
                if (totalCell) {
                    totalCell.textContent = `${itemData.total} ₽`;
                    totalCell.classList.add('updated');
                    setTimeout(() => totalCell.classList.remove('updated'), 500);
                }
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
        // Обработка кликов по кнопкам добавления в корзину
        document.addEventListener('click', (e) => {
            // Добавление в корзину со страницы продуктов
            if (e.target.matches('.add-to-cart-btn') || e.target.closest('.add-to-cart-btn')) {
                const button = e.target.matches('.add-to-cart-btn') ? e.target : e.target.closest('.add-to-cart-btn');
                this.addToCartFromButton(button);
            }

            // Удаление из корзины
            if (e.target.matches('.remove-from-cart') || e.target.closest('.remove-from-cart')) {
                const button = e.target.matches('.remove-from-cart') ? e.target : e.target.closest('.remove-from-cart');
                const productName = button.dataset.productName;
                this.removeFromCart(productName);
            }

            // Очистка корзины
            if (e.target.matches('.clear-cart-btn') || e.target.closest('.clear-cart-btn')) {
                this.clearCart();
            }

            // Кнопки увеличения/уменьшения в корзине
            if (e.target.matches('.cart-increase') || e.target.closest('.cart-increase')) {
                const button = e.target.matches('.cart-increase') ? e.target : e.target.closest('.cart-increase');
                const productName = button.dataset.productName;
                this.updateCartQuantity(productName, 1);
            }

            if (e.target.matches('.cart-decrease') || e.target.closest('.cart-decrease')) {
                const button = e.target.matches('.cart-decrease') ? e.target : e.target.closest('.cart-decrease');
                const productName = button.dataset.productName;
                this.updateCartQuantity(productName, -1);
            }
        });

        // Обработка изменений количества через input в корзине
        document.addEventListener('input', (e) => {
            if (e.target.matches('.cart-quantity-input')) {
                const productName = e.target.dataset.productName;
                const quantity = parseInt(e.target.value) || 0;

                // Дебаунс для избежания множественных запросов
                clearTimeout(this.inputTimeout);
                this.inputTimeout = setTimeout(() => {
                    this.updateCartItem(productName, quantity);
                }, 500);
            }
        });

        // Сохранение корзины при уходе со страницы
        window.addEventListener('beforeunload', () => {
            this.saveCartToStorage();
        });

        // Инициализация кнопок количества на странице продуктов
        this.initProductPageControls();
    }

    initProductPageControls() {
    // Кнопки + и - на странице продуктов
    document.addEventListener('click', (e) => {
        if (e.target.matches('.quantity-btn.plus') || e.target.closest('.quantity-btn.plus')) {
            const button = e.target.matches('.quantity-btn.plus') ? e.target : e.target.closest('.quantity-btn.plus');
            const input = button.previousElementSibling;
            if (input && input.type === 'number' && !input.disabled) {
                const max = parseInt(input.max) || 100;
                const currentValue = parseInt(input.value) || 1;
                if (currentValue < max) {
                    input.value = currentValue + 1;
                    // Генерируем событие change для обновления UI
                    input.dispatchEvent(new Event('change', { bubbles: true }));
                }
            }
            e.stopPropagation(); // Предотвращаем дальнейшую обработку
        }

        if (e.target.matches('.quantity-btn.minus') || e.target.closest('.quantity-btn.minus')) {
            const button = e.target.matches('.quantity-btn.minus') ? e.target : e.target.closest('.quantity-btn.minus');
            const input = button.nextElementSibling;
            if (input && input.type === 'number' && !input.disabled) {
                const min = parseInt(input.min) || 1;
                const currentValue = parseInt(input.value) || 1;
                if (currentValue > min) {
                    input.value = currentValue - 1;
                    // Генерируем событие change для обновления UI
                    input.dispatchEvent(new Event('change', { bubbles: true }));
                }
            }
            e.stopPropagation(); // Предотвращаем дальнейшую обработку
        }
    });

    // Обработка прямого ввода в поле количества
    document.addEventListener('change', (e) => {
        if (e.target.matches('.quantity-input')) {
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

    updateCartQuantity(productName, change) {
        const input = document.querySelector(`input[data-product-name="${productName}"]`);
        if (!input) return;

        let currentValue = parseInt(input.value) || 0;
        const max = parseInt(input.max) || 100;
        const newValue = Math.max(1, Math.min(max, currentValue + change));

        if (newValue !== currentValue) {
            input.value = newValue;

            // Анимация кнопки
            const button = event.target.closest('.quantity-btn-cart');
            if (button) {
                button.classList.add('animate-press');
                setTimeout(() => button.classList.remove('animate-press'), 300);
            }

            // Немедленное обновление
            this.updateCartItem(productName, newValue);
        }
    }

    async removeFromCart(productName) {
        await this.updateCartItem(productName, 0);

        // Анимация удаления
        const row = document.querySelector(`[data-product="${productName}"]`);
        if (row) {
            row.style.transform = 'translateX(100%)';
            row.style.opacity = '0';
            setTimeout(() => {
                if (row.parentNode) {
                    row.parentNode.removeChild(row);
                }

                // Проверяем, пуста ли теперь корзина
                const remainingRows = document.querySelectorAll('[data-product]');
                if (remainingRows.length === 0) {
                    // Перезагружаем страницу, если корзина пуста
                    setTimeout(() => {
                        window.location.reload();
                    }, 500);
                }
            }, 300);
        }

        this.showNotification(`${productName} удален из корзины`, 'success');
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

    updateCartTotal(total) {
        const totalElement = document.getElementById('cart-total');
        if (totalElement) {
            const oldTotal = parseInt(totalElement.textContent.replace('₽', '').trim()) || 0;
            totalElement.textContent = `${total} ₽`;

            // Анимация изменения суммы
            if (total !== oldTotal) {
                totalElement.classList.add('updated');
                setTimeout(() => {
                    totalElement.classList.remove('updated');
                }, 500);
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

        .updated {
            animation: pulse 0.5s ease;
        }

        @keyframes pulse {
            0% { transform: scale(1); }
            50% { transform: scale(1.1); }
            100% { transform: scale(1); }
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