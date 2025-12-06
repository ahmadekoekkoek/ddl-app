"""
GUI Animations Module
Production-grade animation utilities with unified controller and queuing
"""

from PySide6.QtCore import (
    QPropertyAnimation, QEasingCurve, QParallelAnimationGroup,
    QSequentialAnimationGroup, QAbstractAnimation, QPoint, QObject, Signal
)
from PySide6.QtWidgets import QWidget, QGraphicsOpacityEffect
from PySide6.QtGui import QColor

from .constants import ANIMATION


class AnimationController(QObject):
    """
    Unified animation controller with queuing and conflict prevention
    Manages all animations in the application
    """

    # Signals
    animation_started = Signal(str)  # Emits animation ID
    animation_finished = Signal(str)  # Emits animation ID
    queue_emptied = Signal()

    def __init__(self):
        super().__init__()
        self._animation_queue = []
        self._current_animation = None
        self._active_animations = {}  # Track all active animations by ID
        self._animation_counter = 0

    def play(self, animation, animation_id=None, queue=False):
        """
        Play an animation

        Args:
            animation: QAbstractAnimation to play
            animation_id: Optional ID for tracking (auto-generated if None)
            queue: If True, queue animation; if False, play immediately

        Returns:
            str: Animation ID
        """
        if animation_id is None:
            self._animation_counter += 1
            animation_id = f"anim_{self._animation_counter}"

        if queue:
            self._animation_queue.append((animation, animation_id))
            if self._current_animation is None or \
               self._current_animation.state() != QAbstractAnimation.Running:
                self._process_next_animation()
        else:
            self._play_animation(animation, animation_id)

        return animation_id

    def _play_animation(self, animation, animation_id):
        """Internal method to play an animation"""
        # Store in active animations
        self._active_animations[animation_id] = animation

        # Connect finished signal
        animation.finished.connect(lambda: self._on_animation_finished(animation_id))

        # Emit started signal
        self.animation_started.emit(animation_id)

        # Start animation
        animation.start()
        print(f"[AnimationController] Started animation: {animation_id}")

    def _process_next_animation(self):
        """Process next animation in queue"""
        if not self._animation_queue:
            self._current_animation = None
            self.queue_emptied.emit()
            return

        animation, animation_id = self._animation_queue.pop(0)
        self._current_animation = animation
        self._play_animation(animation, animation_id)

    def _on_animation_finished(self, animation_id):
        """Handle animation finished"""
        # Remove from active animations
        if animation_id in self._active_animations:
            del self._active_animations[animation_id]

        # Emit finished signal
        self.animation_finished.emit(animation_id)
        print(f"[AnimationController] Finished animation: {animation_id}")

        # Process next queued animation
        if self._current_animation and \
           self._current_animation.state() != QAbstractAnimation.Running:
            self._process_next_animation()

    def stop(self, animation_id):
        """Stop a specific animation"""
        if animation_id in self._active_animations:
            self._active_animations[animation_id].stop()
            del self._active_animations[animation_id]

    def stop_all(self):
        """Stop all active animations"""
        for anim in list(self._active_animations.values()):
            anim.stop()
        self._active_animations.clear()
        self._animation_queue.clear()
        self._current_animation = None

    def clear_queue(self):
        """Clear all queued animations"""
        self._animation_queue.clear()

    def get_active_count(self):
        """Get number of active animations"""
        return len(self._active_animations)


class AnimationManager:
    """
    Centralized animation manager with pre-built transitions
    Singleton pattern for global access
    """

    _instance = None
    _controller = None

    @classmethod
    def get_instance(cls):
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = cls()
            cls._controller = AnimationController()
        return cls._instance

    @classmethod
    def get_controller(cls):
        """Get animation controller"""
        if cls._controller is None:
            cls.get_instance()
        return cls._controller

    @staticmethod
    def slide_transition(from_widget: QWidget, to_widget: QWidget,
                        direction='right', duration=None):
        """
        Parallax slide transition with cross-fade

        Args:
            from_widget: Widget sliding out
            to_widget: Widget sliding in
            direction: 'right', 'left', 'up', or 'down'
            duration: Animation duration in ms (default from constants)
        """
        if duration is None:
            duration = ANIMATION['duration_normal']

        # Get easing curve
        easing = getattr(QEasingCurve, ANIMATION['easing_smooth'])

        # Create parallel animation group
        group = QParallelAnimationGroup()

        # Calculate offsets based on direction
        width = from_widget.width()
        height = from_widget.height()

        if direction == 'right':
            from_end = QPoint(-width, 0)
            to_start = QPoint(width, 0)
        elif direction == 'left':
            from_end = QPoint(width, 0)
            to_start = QPoint(-width, 0)
        elif direction == 'up':
            from_end = QPoint(0, height)
            to_start = QPoint(0, -height)
        else:  # down
            from_end = QPoint(0, -height)
            to_start = QPoint(0, height)

        # From widget animation
        from_anim = QPropertyAnimation(from_widget, b"pos")
        from_anim.setDuration(duration)
        from_anim.setEasingCurve(easing)
        from_anim.setStartValue(from_widget.pos())
        from_anim.setEndValue(from_end)

        # To widget animation
        to_anim = QPropertyAnimation(to_widget, b"pos")
        to_anim.setDuration(duration)
        to_anim.setEasingCurve(easing)
        to_anim.setStartValue(to_start)
        to_anim.setEndValue(QPoint(0, 0))

        group.addAnimation(from_anim)
        group.addAnimation(to_anim)

        return group

    @staticmethod
    def fade_transition(widget: QWidget, fade_in=True, duration=None):
        """
        Fade in/out animation

        Args:
            widget: Widget to fade
            fade_in: True to fade in, False to fade out
            duration: Animation duration in ms
        """
        if duration is None:
            duration = ANIMATION['duration_fast']

        # Create opacity effect if not exists
        if not widget.graphicsEffect():
            effect = QGraphicsOpacityEffect(widget)
            widget.setGraphicsEffect(effect)
        else:
            effect = widget.graphicsEffect()

        # Create animation
        anim = QPropertyAnimation(effect, b"opacity")
        anim.setDuration(duration)
        anim.setEasingCurve(getattr(QEasingCurve, ANIMATION['easing_default']))

        if fade_in:
            anim.setStartValue(0.0)
            anim.setEndValue(1.0)
        else:
            anim.setStartValue(1.0)
            anim.setEndValue(0.0)

        return anim

    @staticmethod
    def scale_transition(widget: QWidget, scale_from=1.0, scale_to=1.0, duration=None):
        """
        Scale animation with size change

        Args:
            widget: Widget to scale
            scale_from: Starting scale factor
            scale_to: Target scale factor
            duration: Animation duration in ms
        """
        if duration is None:
            duration = ANIMATION['duration_fast']

        # Create size animation
        anim = QPropertyAnimation(widget, b"size")
        anim.setDuration(duration)
        anim.setEasingCurve(getattr(QEasingCurve, ANIMATION['easing_default']))

        current_size = widget.size()
        start_size = current_size * scale_from
        end_size = current_size * scale_to

        anim.setStartValue(start_size)
        anim.setEndValue(end_size)

        return anim

    @staticmethod
    def reveal_transition(widget: QWidget, direction='down', duration=None):
        """
        Reveal animation (slide + fade)

        Args:
            widget: Widget to reveal
            direction: 'up', 'down', 'left', 'right'
            duration: Animation duration in ms
        """
        if duration is None:
            duration = ANIMATION['duration_normal']

        # Create parallel group for slide + fade
        group = QParallelAnimationGroup()

        # Fade animation
        fade_anim = AnimationManager.fade_transition(widget, fade_in=True, duration=duration)
        group.addAnimation(fade_anim)

        # Slide animation
        offset = 20  # Slide distance in pixels

        if direction == 'down':
            start_pos = widget.pos() - QPoint(0, offset)
        elif direction == 'up':
            start_pos = widget.pos() + QPoint(0, offset)
        elif direction == 'right':
            start_pos = widget.pos() - QPoint(offset, 0)
        else:  # left
            start_pos = widget.pos() + QPoint(offset, 0)

        slide_anim = QPropertyAnimation(widget, b"pos")
        slide_anim.setDuration(duration)
        slide_anim.setEasingCurve(getattr(QEasingCurve, ANIMATION['easing_smooth']))
        slide_anim.setStartValue(start_pos)
        slide_anim.setEndValue(widget.pos())

        group.addAnimation(slide_anim)

        return group

    @staticmethod
    def bounce_transition(widget: QWidget, duration=None):
        """
        Bounce animation for emphasis

        Args:
            widget: Widget to bounce
            duration: Animation duration in ms
        """
        if duration is None:
            duration = ANIMATION['duration_fast']

        # Create sequential animation with multiple bounces
        sequence = QSequentialAnimationGroup()

        original_pos = widget.pos()
        bounce_height = 10  # pixels

        # Bounce up
        up_anim = QPropertyAnimation(widget, b"pos")
        up_anim.setDuration(duration // 3)
        up_anim.setEasingCurve(QEasingCurve.OutQuad)
        up_anim.setStartValue(original_pos)
        up_anim.setEndValue(original_pos - QPoint(0, bounce_height))

        # Bounce down
        down_anim = QPropertyAnimation(widget, b"pos")
        down_anim.setDuration(duration // 3)
        down_anim.setEasingCurve(QEasingCurve.InQuad)
        down_anim.setStartValue(original_pos - QPoint(0, bounce_height))
        down_anim.setEndValue(original_pos)

        # Small bounce
        small_up = QPropertyAnimation(widget, b"pos")
        small_up.setDuration(duration // 6)
        small_up.setEasingCurve(QEasingCurve.OutQuad)
        small_up.setStartValue(original_pos)
        small_up.setEndValue(original_pos - QPoint(0, bounce_height // 2))

        small_down = QPropertyAnimation(widget, b"pos")
        small_down.setDuration(duration // 6)
        small_down.setEasingCurve(QEasingCurve.InQuad)
        small_down.setStartValue(original_pos - QPoint(0, bounce_height // 2))
        small_down.setEndValue(original_pos)

        sequence.addAnimation(up_anim)
        sequence.addAnimation(down_anim)
        sequence.addAnimation(small_up)
        sequence.addAnimation(small_down)

        return sequence

    @staticmethod
    def pulse_transition(widget: QWidget, duration=None, pulses=2):
        """
        Pulse animation (scale up and down)

        Args:
            widget: Widget to pulse
            duration: Animation duration in ms
            pulses: Number of pulses
        """
        if duration is None:
            duration = ANIMATION['duration_normal']

        sequence = QSequentialAnimationGroup()

        pulse_duration = duration // (pulses * 2)

        for _ in range(pulses):
            # Scale up
            scale_up = AnimationManager.scale_transition(
                widget, scale_from=1.0, scale_to=1.1, duration=pulse_duration
            )
            # Scale down
            scale_down = AnimationManager.scale_transition(
                widget, scale_from=1.1, scale_to=1.0, duration=pulse_duration
            )

            sequence.addAnimation(scale_up)
            sequence.addAnimation(scale_down)

        return sequence
