import './ToggleSwitch.css';

interface ToggleSwitchProps {
  leftLabel: string;
  rightLabel: string;
  isRight: boolean;
  onChange: (isRight: boolean) => void;
  disabled?: boolean;
}

export function ToggleSwitch({
  leftLabel,
  rightLabel,
  isRight,
  onChange,
  disabled = false,
}: ToggleSwitchProps) {
  return (
    <div className={`toggle-switch ${disabled ? 'disabled' : ''}`}>
      <span
        className={`toggle-label ${!isRight ? 'active' : ''}`}
        onClick={() => !disabled && onChange(false)}
      >
        {leftLabel}
      </span>
      <button
        className={`toggle-track ${isRight ? 'right' : 'left'}`}
        onClick={() => !disabled && onChange(!isRight)}
        disabled={disabled}
        type="button"
      >
        <span className="toggle-thumb" />
      </button>
      <span
        className={`toggle-label ${isRight ? 'active' : ''}`}
        onClick={() => !disabled && onChange(true)}
      >
        {rightLabel}
      </span>
    </div>
  );
}
