import { forwardRef, type ButtonHTMLAttributes } from 'react'
import { cva, type VariantProps } from 'class-variance-authority'
import { cn } from '../../lib/utils'

const buttonStyles = cva('inline-flex items-center justify-center gap-2 transition-colors disabled:pointer-events-none disabled:opacity-50', {
  variants: {
    variant: { solid: 'solid-cta', outline: 'outline-cta', quiet: 'quiet-button' },
    size: { default: '', small: 'button-small' },
  },
  defaultVariants: { variant: 'solid', size: 'default' },
})

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement>, VariantProps<typeof buttonStyles> {}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(({ className, variant, size, ...props }, ref) => (
  <button ref={ref} className={cn(buttonStyles({ variant, size, className }))} {...props}/>
))
Button.displayName = 'Button'
